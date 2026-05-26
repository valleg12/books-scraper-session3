"""
scraper.py — Pipeline de scraping books.toscrape.com (version optimisée)
=========================================================================
Architecture en 2 phases entièrement parallèles pour maximiser la vitesse :

  PHASE 1 — Catalogue (50 req. EN PARALLÈLE)
    Les 50 pages catalogue ont des URLs prévisibles (page-1.html … page-50.html).
    On les télécharge TOUTES EN MÊME TEMPS plutôt que séquentiellement.
    → Gain : 8s séquentiel → ~1s parallèle

  PHASE 2 — Fiches livres (1000 req. EN PARALLÈLE)
    Toutes les fiches sont scrapées EN PARALLÈLE avec un grand pool de workers.

  TOTAL : ~1050 requêtes HTTP, objectif < 11 secondes sur tout le site.

Optimisations clés vs session1 :
  - requests.Session + HTTPAdapter : connexions TCP réutilisées (connection pooling)
  - Architecture 2 phases : zéro attente entre catalogue et fiches
  - Compteur de requêtes en temps réel
  - Backoff exponentiel sur les retries
  - Arrêt propre sur Ctrl+C / docker stop (SIGTERM)
"""

# ── Imports bibliothèque standard ─────────────────────────────────────────────
import argparse          # Arguments CLI (--output, --workers, etc.)
import csv               # Écriture du CSV de sortie
import logging           # Logs structurés avec timestamp et niveau
import os                # Vérification espace disque
import signal            # Interception SIGTERM (docker stop) et SIGINT (Ctrl+C)
import sys               # Accès aux args et sortie du programme
import time              # Mesure des durées, pauses entre retries
from concurrent.futures import ThreadPoolExecutor, as_completed  # Parallélisation
from datetime import date        # Horodatage de chaque ligne collectée
from pathlib import Path         # Manipulation des chemins de fichiers
from threading import Lock       # Protection du compteur de requêtes (multi-thread)

# ── Imports tiers ─────────────────────────────────────────────────────────────
import requests                  # Requêtes HTTP
from requests.adapters import HTTPAdapter  # Configuration du pool de connexions
from bs4 import BeautifulSoup    # Parsing HTML


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

CATALOGUE_URL   = "https://books.toscrape.com/catalogue/"

# Nombre total de pages catalogue (fixe sur books.toscrape.com)
# Utilisé pour générer toutes les URLs en Phase 1 sans navigation séquentielle
CATALOGUE_PAGES = 50

# Colonnes du CSV de sortie (ordre fixé ici)
FIELDNAMES      = ["title", "price", "rating", "category", "date"]

# Nombre de livres accumulés avant écriture disque
BATCH_SIZE      = 100

# Retry : nombre de tentatives et délai de base (doublé à chaque échec)
MAX_RETRIES     = 3
RETRY_DELAY     = 2   # secondes — backoff exponentiel : 2s, 4s, 8s

# Workers : bornes pour éviter les valeurs absurdes passées en CLI
MIN_WORKERS     = 1
MAX_WORKERS     = 100  # augmenté pour permettre plus de parallélisme

# Espace disque minimum requis avant d'écrire (en Mo)
MIN_DISK_MB    = 10

# Mapping note textuelle → entier (classe CSS du site → valeur numérique)
RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# Format : 2026-05-19 10:00:01 | INFO     | message
# Visible dans le terminal ET dans "docker logs session3_scraper"
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,  # stdout = visible dans docker logs
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# COMPTEUR DE REQUÊTES (thread-safe)
# Partagé entre tous les threads, protégé par un Lock pour éviter les conflits
# ══════════════════════════════════════════════════════════════════════════════

_request_count = 0           # Nombre total de requêtes HTTP effectuées
_request_lock  = Lock()      # Verrou pour écriture thread-safe


def _count_request() -> int:
    """Incrémente le compteur global et retourne la nouvelle valeur."""
    global _request_count
    with _request_lock:
        _request_count += 1
        return _request_count


# ══════════════════════════════════════════════════════════════════════════════
# ARRÊT PROPRE (SIGTERM / Ctrl+C)
# ══════════════════════════════════════════════════════════════════════════════

_shutdown = False  # Flag global : True = arrêt propre demandé


def _handle_signal(signum, _frame):
    """Positionne le flag d'arrêt sans quitter immédiatement."""
    global _shutdown
    name = "SIGTERM" if signum == signal.SIGTERM else "Ctrl+C"
    logger.warning("[SIGNAL] %s recu — les données collectées seront sauvegardées", name)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ══════════════════════════════════════════════════════════════════════════════
# SESSION HTTP AVEC CONNECTION POOLING
# Sans Session : chaque requête ouvre une nouvelle connexion TCP (+100ms/req)
# Avec Session : les connexions TCP sont réutilisées (HTTP keep-alive)
# Sur 1000 requêtes, le gain est de plusieurs secondes.
# ══════════════════════════════════════════════════════════════════════════════

def build_session(workers: int) -> requests.Session:
    """
    Crée une Session requests avec un pool de connexions dimensionné
    au nombre de workers. Chaque thread a sa propre connexion disponible.
    """
    session = requests.Session()

    # pool_connections : nombre de hosts différents en parallèle (ici 1 = books.toscrape.com)
    # pool_maxsize     : nombre de connexions simultanées vers ce host
    adapter = HTTPAdapter(
        pool_connections=workers,
        pool_maxsize=workers * 2,  # *2 pour absorber les pics
        max_retries=0,             # On gère nous-mêmes les retries
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ══════════════════════════════════════════════════════════════════════════════
# REQUÊTES HTTP
# ══════════════════════════════════════════════════════════════════════════════

def fetch(url: str, session: requests.Session) -> requests.Response | None:
    """
    Requête GET avec retry et backoff exponentiel.

    Backoff exponentiel = délai doublé à chaque échec :
      1ère erreur → attente 2s
      2ème erreur → attente 4s
      3ème erreur → abandon

    Gestion des cas d'erreur :
      - ConnectionError  : réseau coupé / DNS en panne
      - Timeout          : serveur trop lent (seuil : 10s)
      - HTTPError 429    : rate limiting → attente renforcée
      - HTTPError 404    : page inexistante → pas de retry
      - SSLError         : certificat invalide → pas de retry
      - TooManyRedirects : boucle infinie → pas de retry
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                url,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (scraper-session3/2.0)"},
            )
            resp.raise_for_status()
            _count_request()  # Incrémenter le compteur après chaque succès
            return resp

        except requests.exceptions.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            if code == 429:
                # Rate limiting : on attend beaucoup plus longtemps
                wait = RETRY_DELAY * (2 ** attempt) * 2
                logger.warning("[HTTP] 429 Rate Limit — attente %ds avant retry (%d/%d) : %s",
                               wait, attempt, MAX_RETRIES, url)
                time.sleep(wait)
            elif code in (404, 410):
                # Page inexistante ou supprimée définitivement : inutile de réessayer
                logger.debug("[HTTP] %d — page absente (skip) : %s", code, url)
                return None
            else:
                wait = RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning("[HTTP] %d — tentative %d/%d, attente %ds : %s",
                               code, attempt, MAX_RETRIES, wait, url)
                if attempt < MAX_RETRIES:
                    time.sleep(wait)

        except requests.exceptions.SSLError:
            logger.error("[SSL] Certificat invalide (skip) : %s", url)
            return None

        except requests.exceptions.TooManyRedirects:
            logger.error("[REDIRECT] Boucle de redirections (skip) : %s", url)
            return None

        except requests.exceptions.ConnectionError:
            wait = RETRY_DELAY * (2 ** (attempt - 1))
            logger.warning("[RESEAU] Connexion impossible — tentative %d/%d, attente %ds",
                           attempt, MAX_RETRIES, wait)
            if attempt < MAX_RETRIES:
                time.sleep(wait)

        except requests.exceptions.Timeout:
            wait = RETRY_DELAY * (2 ** (attempt - 1))
            logger.warning("[TIMEOUT] Pas de réponse — tentative %d/%d, attente %ds",
                           attempt, MAX_RETRIES, wait)
            if attempt < MAX_RETRIES:
                time.sleep(wait)

        except Exception as exc:
            logger.error("[RESEAU] Erreur inattendue sur %s : %s", url, exc)
            return None

    logger.error("[ABANDON] %d tentatives epuisees : %s", MAX_RETRIES, url)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# PARSING HTML
# ══════════════════════════════════════════════════════════════════════════════

def parse_catalogue_page(soup: BeautifulSoup) -> tuple[list[str], str | None]:
    """
    Depuis une page catalogue, extrait :
      - La liste des URLs complètes des 20 fiches livres
      - L'URL de la page suivante (None si on est sur la dernière)

    Structure HTML d'une page catalogue :
      <article class="product_pod">
        <a href="../../a-light-in-the-attic.../index.html">  ← lien relatif
      </article>
      <li class="next"><a href="page-2.html">
    """
    # Extraire les URLs des livres (liens relatifs → URL absolues)
    book_urls = []
    for article in soup.select("article.product_pod"):
        try:
            relative = article.find("a")["href"]
            # Les liens contiennent "../../" qu'il faut supprimer
            clean = relative.replace("../", "")
            book_urls.append(CATALOGUE_URL + clean)
        except (TypeError, KeyError):
            continue  # Article sans lien valide, on ignore

    # Trouver le lien vers la page suivante
    next_btn = soup.select_one("li.next a")
    next_url = (CATALOGUE_URL + next_btn["href"]) if next_btn else None

    return book_urls, next_url


def parse_book(soup: BeautifulSoup, url: str, scrape_date: str) -> dict | None:
    """
    Depuis la page d'un livre, extrait :
      title    ← balise <h1>
      price    ← <p class="price_color">
      rating   ← classe CSS de <p class="star-rating Three"> → chiffre via RATING_MAP
      category ← 3ème élément du fil d'Ariane <ul class="breadcrumb">
      date     ← date du jour passée en paramètre

    Retourne None si un champ obligatoire est introuvable.
    """
    try:
        # Titre
        h1 = soup.find("h1")
        if not h1:
            raise ValueError("<h1> introuvable")
        title = h1.text.strip()

        # Prix (conservé tel quel avec le symbole £)
        price_tag = soup.find("p", class_="price_color")
        if not price_tag:
            raise ValueError("<p class='price_color'> introuvable")
        price = price_tag.text.strip()

        # Note : la classe CSS contient le mot "Three", "Five", etc.
        rating_tag = soup.find("p", class_="star-rating")
        rating_word = rating_tag.get("class", ["", "Zero"])[1] if rating_tag else "Zero"
        rating = RATING_MAP.get(rating_word, 0)

        # Catégorie via fil d'Ariane : [Home, Categorie, Titre du livre]
        breadcrumb = soup.select("ul.breadcrumb li")
        category = breadcrumb[2].text.strip() if len(breadcrumb) >= 3 else "Unknown"

        return {"title": title, "price": price, "rating": rating,
                "category": category, "date": scrape_date}

    except ValueError as exc:
        logger.warning("[PARSE] Structure manquante — %s : %s", exc, url)
        return None
    except Exception as exc:
        logger.error("[PARSE] Erreur inattendue sur %s : %s", url, exc)
        return None


def scrape_book(url: str, scrape_date: str, session: requests.Session) -> dict | None:
    """Télécharge + parse une fiche livre. Appelé par les threads workers."""
    resp = fetch(url, session)
    if resp is None:
        return None
    resp.encoding = "utf-8"
    return parse_book(BeautifulSoup(resp.text, "html.parser"), url, scrape_date)


# ══════════════════════════════════════════════════════════════════════════════
# PERSISTANCE CSV
# ══════════════════════════════════════════════════════════════════════════════

def init_csv(path: Path) -> None:
    """Réinitialise le CSV à chaque run — écrase l'ancien fichier pour ne pas accumuler les données."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()
    logger.info("[CSV] Fichier réinitialisé : %s", path)


def flush_csv(batch: list[dict], path: Path) -> bool:
    """Écrit un lot en mode append. Retourne False si erreur disque."""
    if not batch:
        return True
    # Vérification espace disque avant écriture
    try:
        disk = os.statvfs(path.parent)
        free_mb = disk.f_bavail * disk.f_frsize / 1_048_576
        if free_mb < MIN_DISK_MB:
            logger.error("[CSV] Disque presque plein (%.1f Mo) — écriture annulée", free_mb)
            return False
    except Exception:
        pass  # Impossible de vérifier → on tente quand même
    try:
        t0 = time.time()
        with path.open("a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writerows(batch)
        logger.info("[CSV] %d livres écrits en %.2fs → %s", len(batch), time.time() - t0, path)
        return True
    except PermissionError:
        logger.error("[CSV] Fichier verrouillé (ouvert dans un autre programme ?)")
        return False
    except OSError as exc:
        logger.error("[CSV] Erreur d'écriture : %s", exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATIONS PRÉALABLES
# ══════════════════════════════════════════════════════════════════════════════

def check_connectivity(session: requests.Session) -> bool:
    """Vérifie la connectivité vers le site avant de lancer le scraping."""
    logger.info("[INIT] Test de connectivité...")
    try:
        r = session.get("https://books.toscrape.com/", timeout=5)
        r.raise_for_status()
        logger.info("[INIT] Site accessible (HTTP %d)", r.status_code)
        return True
    except Exception as exc:
        logger.error("[INIT] Site inaccessible : %s", exc)
        return False


def validate_output(path: Path) -> bool:
    """Vérifie qu'on peut écrire dans le dossier de destination."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not os.access(path.parent, os.W_OK):
            logger.error("[INIT] Pas de permission d'écriture dans : %s", path.parent)
            return False
        logger.info("[INIT] Chemin de sortie valide : %s", path)
        return True
    except Exception as exc:
        logger.error("[INIT] Chemin invalide : %s", exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL — ARCHITECTURE 2 PHASES
# ══════════════════════════════════════════════════════════════════════════════

def run(output_path: Path, max_pages: int | None, workers: int) -> dict:
    """
    Exécute le pipeline complet en 2 phases.

    PHASE 1 — Collecte des URLs (catalogue)
    ────────────────────────────────────────
    On parcourt toutes les pages catalogue pour récupérer les URLs
    des 1000 fiches livres. Les pages catalogue sont téléchargées
    séquentiellement (on a besoin de l'URL de la page suivante
    pour savoir quoi demander ensuite).

    PHASE 2 — Scraping des fiches (parallèle massif)
    ─────────────────────────────────────────────────
    Toutes les URLs collectées sont scrapées EN MÊME TEMPS
    avec un pool de workers. C'est ici que se joue la performance :
    plus de workers = moins de temps d'attente réseau global.

    Retourne un dictionnaire de statistiques.
    """
    global _request_count, _shutdown

    # Session HTTP partagée entre tous les threads
    session = build_session(workers)
    scrape_date = date.today().isoformat()

    n_pages = max_pages or CATALOGUE_PAGES
    logger.info("=" * 60)
    logger.info("PIPELINE DÉMARRÉ")
    logger.info("  Date          : %s", scrape_date)
    logger.info("  Max pages     : %s", max_pages or f"toutes ({CATALOGUE_PAGES})")
    logger.info("  Workers       : %d", workers)
    logger.info("  Sortie        : %s", output_path)
    logger.info("  Requêtes max  : %d catalogue + %d fiches ≈ %d total",
                n_pages, n_pages * 20, n_pages * 21)
    logger.info("  Phase 1       : PARALLÈLE (%d workers)", min(workers, n_pages))
    logger.info("  Phase 2       : PARALLÈLE (%d workers)", workers)
    logger.info("=" * 60)

    pipeline_start = time.time()

    # Réinitialiser le CSV dès le départ — avant même Phase 1
    init_csv(output_path)

    # ── PHASE 1 : Collecte des URLs du catalogue (PARALLÈLE) ─────────────────
    #
    # OPTIMISATION CLÉ : les URLs des pages catalogue sont prévisibles.
    # Au lieu de paginer séquentiellement (page 1 → lire "suivant" → page 2…),
    # on génère directement les 50 URLs et on les télécharge TOUTES EN MÊME TEMPS.
    #
    # Gain : ~8s (séquentiel) → ~1s (parallèle sur 50 workers)
    logger.info("[PHASE 1] Collecte PARALLÈLE des URLs du catalogue...")
    phase1_start = time.time()

    num_pages  = max_pages if max_pages is not None else CATALOGUE_PAGES
    cat_urls   = [CATALOGUE_URL + f"page-{i}.html" for i in range(1, num_pages + 1)]

    all_book_urls: list[str] = []

    def _fetch_catalogue_page(url: str) -> tuple[str, list[str]]:
        """Télécharge une page catalogue et retourne (url, liste_urls_livres)."""
        resp = fetch(url, session)
        if resp is None:
            return url, []
        resp.encoding = "utf-8"
        book_urls, _ = parse_catalogue_page(BeautifulSoup(resp.text, "html.parser"))
        return url, book_urls

    # pool_cat = min(workers, num_pages) pour ne pas ouvrir plus de connexions que nécessaire
    pool_cat = min(workers, num_pages)
    with ThreadPoolExecutor(max_workers=pool_cat) as executor:
        cat_futures = {executor.submit(_fetch_catalogue_page, u): u for u in cat_urls}
        # Récolter les résultats dans l'ordre d'arrivée
        page_results: dict[str, list[str]] = {}
        for future in as_completed(cat_futures):
            if _shutdown:
                break
            url, book_urls = future.result()
            page_results[url] = book_urls

    # Reconstituer all_book_urls dans l'ordre original des pages
    for url in cat_urls:
        all_book_urls.extend(page_results.get(url, []))

    pages_ok = sum(1 for u in cat_urls if page_results.get(u))

    phase1_duration = time.time() - phase1_start
    logger.info("[PHASE 1] Terminée — %d/%d pages, %d URLs collectées en %.2fs",
                pages_ok, num_pages, len(all_book_urls), phase1_duration)
    logger.info("[PHASE 1] Requêtes effectuées : %d", _request_count)

    if not all_book_urls:
        logger.error("[PHASE 1] Aucune URL collectée — arrêt")
        return {"total": 0, "errors": 0, "requests": _request_count, "duration": 0}

    # ── PHASE 2 : Scraping parallèle de toutes les fiches ────────────────────
    logger.info("[PHASE 2] Scraping de %d fiches avec %d workers...", len(all_book_urls), workers)
    phase2_start = time.time()

    total_ok     = 0   # Livres scrapés avec succès
    total_errors = 0   # Livres en échec
    batch: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Soumettre toutes les fiches d'un seul coup au pool
        futures = {
            executor.submit(scrape_book, url, scrape_date, session): url
            for url in all_book_urls
        }

        for future in as_completed(futures):
            if _shutdown:
                # Annuler les futures pas encore démarrés
                for f in futures:
                    f.cancel()
                break

            url = futures[future]
            try:
                book = future.result()
            except Exception as exc:
                logger.error("[PHASE 2] Exception thread sur %s : %s", url, exc)
                total_errors += 1
                continue

            if book is None:
                total_errors += 1
                continue

            total_ok += 1
            batch.append(book)
            logger.info("[LIVRE %4d] %-50s | %s | %d* | %s",
                        total_ok,
                        book["title"][:50],
                        book["price"],
                        book["rating"],
                        book["category"])

            # Écriture par lots pour ne pas rester trop longtemps en mémoire
            if len(batch) >= BATCH_SIZE:
                flush_csv(batch, output_path)
                batch = []

    # Écrire les livres restants (< BATCH_SIZE)
    if batch:
        flush_csv(batch, output_path)

    phase2_duration = time.time() - phase2_start
    total_duration  = time.time() - pipeline_start
    total_requests  = _request_count
    rps             = total_requests / total_duration if total_duration > 0 else 0
    success_rate    = total_ok / (total_ok + total_errors) * 100 if (total_ok + total_errors) > 0 else 0

    logger.info("=" * 60)
    logger.info("PIPELINE %s", "INTERROMPU" if _shutdown else "TERMINÉ")
    logger.info("  Phase 1 (catalogue)  : %.2fs", phase1_duration)
    logger.info("  Phase 2 (fiches)     : %.2fs", phase2_duration)
    logger.info("  ─────────────────────────────────────────")
    logger.info("  Durée totale         : %.2fs", total_duration)
    logger.info("  Requêtes HTTP        : %d", total_requests)
    logger.info("  Débit                : %.1f req/s", rps)
    logger.info("  ─────────────────────────────────────────")
    logger.info("  Livres collectés     : %d", total_ok)
    logger.info("  Erreurs              : %d", total_errors)
    logger.info("  Taux de succès       : %.1f%%", success_rate)
    logger.info("  Fichier CSV          : %s", output_path)
    logger.info("=" * 60)

    return {
        "total":    total_ok,
        "errors":   total_errors,
        "requests": total_requests,
        "duration": total_duration,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ARGUMENTS CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scraper books.toscrape.com → CSV (2 phases, ~1050 req)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--output",    type=Path, default=Path("data/books.csv"),
                        help="Chemin du fichier CSV de sortie")
    parser.add_argument("--max-pages", type=int, default=0, metavar="N",
                        help="Nb max de pages (0 = tout le site, 50 pages)")
    parser.add_argument("--workers",   type=int, default=50, metavar="N",
                        help=f"Workers parallèles pour catalogue + fiches ({MIN_WORKERS}-{MAX_WORKERS})")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> bool:
    """Corrige les valeurs hors limites sans planter."""
    if args.workers < MIN_WORKERS:
        logger.warning("[ARGS] workers=%d corrigé à %d", args.workers, MIN_WORKERS)
        args.workers = MIN_WORKERS
    elif args.workers > MAX_WORKERS:
        logger.warning("[ARGS] workers=%d corrigé à %d", args.workers, MAX_WORKERS)
        args.workers = MAX_WORKERS

    # 0 = tout le site (None dans la logique interne)
    if args.max_pages <= 0:
        args.max_pages = None
    return True


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()
    validate_args(args)

    logger.info("[INIT] workers=%d | max_pages=%s | output=%s",
                args.workers, args.max_pages or "tout", args.output)

    # Vérifications préalables avec une session temporaire
    session_test = build_session(1)
    if not check_connectivity(session_test) or not validate_output(args.output):
        sys.exit(1)

    try:
        stats = run(args.output, args.max_pages, args.workers)
        # Code 1 si plus de 50% d'erreurs
        total = stats["total"] + stats["errors"]
        if total > 0 and stats["errors"] / total > 0.5:
            logger.warning("[FIN] Taux d'erreur > 50%% — vérifier réseau ou site")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("[FIN] Interrompu")
        sys.exit(0)
    except MemoryError:
        logger.error("[FIN] Mémoire insuffisante — réduire --workers")
        sys.exit(1)
    except Exception as exc:
        logger.error("[FIN] Erreur fatale : %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
