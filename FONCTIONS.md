# Documentation des fonctions — scraper.py
## Projet : Pipeline de scraping books.toscrape.com
**Auteur :** Victorien Alleg — Eugenia School

---

## Architecture générale

Le scraper fonctionne en **2 phases parallèles** pour maximiser la vitesse :

```
DÉMARRAGE
   │
   ├─ check_connectivity()   →  vérifie que le site est accessible
   ├─ validate_output()       →  vérifie qu'on peut écrire le CSV
   └─ init_csv()              →  remet le CSV à zéro

PHASE 1 — Catalogue (50 requêtes en parallèle)
   │
   ├─ build_session()         →  crée le pool de connexions HTTP
   ├─ _fetch_catalogue_page() →  télécharge 1 page catalogue
   ├─ fetch()                 →  requête HTTP avec retry
   └─ parse_catalogue_page()  →  extrait les 20 URLs de livres

PHASE 2 — Fiches livres (1000 requêtes en parallèle)
   │
   ├─ scrape_book()           →  télécharge + parse 1 fiche livre
   ├─ fetch()                 →  (même fonction que Phase 1)
   ├─ parse_book()            →  extrait titre, prix, note, catégorie
   └─ flush_csv()             →  écrit les livres en lots dans le CSV

RÉSULTAT : books.csv avec 1000 lignes en ~10 secondes
```

---

## Fonctions détaillées

### `build_session(workers: int) → requests.Session`

**Rôle :** Crée une session HTTP avec un pool de connexions.

**Pourquoi ?** Sans session, chaque requête ouvre une nouvelle connexion TCP (coûte ~100ms). Avec une session, les connexions sont réutilisées (HTTP keep-alive). Sur 1000 requêtes, le gain est de plusieurs secondes.

**Paramètres :**
- `workers` : nombre de threads → dimensionne le pool en conséquence

**Exemple :**
```python
session = build_session(50)  # pool de 50 connexions simultanées
```

---

### `_count_request() → int`

**Rôle :** Incrémente le compteur global de requêtes HTTP de façon thread-safe.

**Pourquoi un Lock ?** Plusieurs threads s'exécutent simultanément. Sans protection, deux threads pourraient lire la même valeur (ex: 42), l'incrémenter chacun de leur côté, et écrire 43 deux fois au lieu de 44. Le `Lock` garantit qu'un seul thread à la fois modifie le compteur.

```python
# Sans Lock (FAUX) :  thread A lit 42, thread B lit 42, tous deux écrivent 43
# Avec Lock (CORRECT) : thread A lit 42, écrit 43, thread B lit 43, écrit 44
```

---

### `_handle_signal(signum, _frame)`

**Rôle :** Intercepte les signaux d'arrêt pour sauvegarder les données avant de quitter.

**Signaux gérés :**
- `SIGTERM` : envoyé par `docker stop` — Docker attend 10s avant de forcer
- `SIGINT` : envoyé par Ctrl+C dans le terminal

**Comportement :** Positionne le flag `_shutdown = True`. Le pipeline détecte ce flag à chaque itération et s'arrête proprement (les données déjà collectées sont sauvegardées).

---

### `fetch(url: str, session: requests.Session) → Response | None`

**Rôle :** Effectue une requête HTTP GET avec **retry et backoff exponentiel**.

**Backoff exponentiel :**
```
1ère erreur → attente 2s  (2^0 × 2)
2ème erreur → attente 4s  (2^1 × 2)
3ème erreur → abandon
```

**Erreurs gérées :**
| Erreur | Comportement |
|--------|-------------|
| `ConnectionError` | Retry avec backoff |
| `Timeout` (>10s) | Retry avec backoff |
| `HTTP 429` (rate limit) | Attente longue + retry |
| `HTTP 404` (page absente) | Skip immédiat (pas de retry) |
| `SSLError` | Skip immédiat |
| `TooManyRedirects` | Skip immédiat |

**Retourne :** L'objet `Response` en cas de succès, `None` en cas d'échec définitif.

---

### `parse_catalogue_page(soup) → tuple[list[str], str | None]`

**Rôle :** Extrait depuis une page catalogue HTML :
1. La liste des 20 URLs de fiches livres
2. L'URL de la page suivante (ou `None` si dernière page)

**Structure HTML ciblée :**
```html
<article class="product_pod">
  <a href="../../a-light-in-the-attic.../index.html">
</article>
<li class="next"><a href="page-2.html">
```

**Nettoyage des URLs :** Les liens contiennent `../../` qu'on supprime pour obtenir une URL absolue valide.

---

### `parse_book(soup, url, scrape_date) → dict | None`

**Rôle :** Extrait les données d'une fiche livre HTML.

**Données extraites :**
| Champ | Balise HTML ciblée |
|-------|-------------------|
| `title` | `<h1>` |
| `price` | `<p class="price_color">` |
| `rating` | classe CSS de `<p class="star-rating">` |
| `category` | 3ème élément du fil d'Ariane `<ul class="breadcrumb">` |
| `date` | Date du jour (passée en paramètre) |

**Conversion des notes :** La note est dans la classe CSS (`"star-rating Three"` → `3`). On utilise un dictionnaire `RATING_MAP` pour convertir le mot en chiffre.

**Retourne :** Un dictionnaire avec les 5 champs, ou `None` si une balise obligatoire est absente.

---

### `scrape_book(url, scrape_date, session) → dict | None`

**Rôle :** Combine `fetch()` + `parse_book()` en une seule fonction appelée par les threads workers.

C'est la fonction soumise au `ThreadPoolExecutor` : chaque worker exécute `scrape_book()` sur une URL différente simultanément.

---

### `init_csv(path: Path)`

**Rôle :** Réinitialise le fichier CSV à chaque lancement — écrase l'ancien fichier.

**Pourquoi écraser ?** Pour ne pas accumuler les données entre deux runs. Chaque exécution repart de zéro avec un fichier propre.

**Mode `"w"` (write) :** Tronque le fichier existant et écrit uniquement l'en-tête.

---

### `flush_csv(batch: list[dict], path: Path) → bool`

**Rôle :** Écrit un lot de livres en mode append dans le CSV.

**Pourquoi des lots (BATCH_SIZE = 100) ?** Écrire ligne par ligne = 1000 ouvertures/fermetures de fichier. Écrire par lots = 10 opérations. Plus efficace en I/O disque.

**Mode `"a"` (append) :** Ajoute à la suite du fichier sans écraser (l'en-tête a déjà été écrit par `init_csv`).

**Vérifications :** Contrôle l'espace disque disponible (minimum 10 Mo) avant d'écrire.

---

### `check_connectivity(session) → bool`

**Rôle :** Vérifie que le site `books.toscrape.com` est accessible avant de lancer le scraping.

Évite de démarrer un pipeline de 1050 requêtes si le réseau est coupé ou le site en maintenance.

---

### `validate_output(path) → bool`

**Rôle :** Vérifie que le dossier de destination existe et qu'on a la permission d'y écrire.

Évite un crash en plein milieu du scraping si le chemin est invalide.

---

### `run(output_path, max_pages, workers) → dict`

**Rôle :** Orchestre l'ensemble du pipeline. C'est la fonction principale.

**Flux d'exécution :**
1. Crée la session HTTP (`build_session`)
2. Réinitialise le CSV (`init_csv`)
3. **Phase 1** : génère toutes les URLs catalogue → `ThreadPoolExecutor` → récupère 1000 URLs de livres
4. **Phase 2** : soumet toutes les URLs à un `ThreadPoolExecutor` → `scrape_book` en parallèle → `flush_csv` par lots
5. Calcule et affiche les statistiques finales

**Retourne :** `{"total": 1000, "errors": 0, "requests": 1050, "duration": 10.2}`

---

### `parse_args()` et `validate_args()`

**Rôle :** Gèrent les arguments de la ligne de commande.

```bash
python scraper.py --output data/books.csv --max-pages 0 --workers 50
#                  ↑ chemin CSV            ↑ 0=tout le site  ↑ threads parallèles
```

`validate_args()` corrige silencieusement les valeurs hors limites (ex: `--workers 200` → corrigé à 100) au lieu de planter.

---

## Concepts clés à retenir

| Concept | Explication rapide |
|---------|-------------------|
| `ThreadPoolExecutor` | Exécute N fonctions en parallèle sur N threads |
| `as_completed()` | Récupère les résultats au fur et à mesure qu'ils arrivent |
| `requests.Session` | Réutilise les connexions TCP (keep-alive) |
| `HTTPAdapter` | Configure le pool de connexions (taille, retries) |
| `threading.Lock` | Protège une variable partagée entre threads |
| `signal.signal()` | Intercepte Ctrl+C et `docker stop` pour arrêt propre |
| Backoff exponentiel | Délai d'attente doublé à chaque retry (2s → 4s → 8s) |
| Mode `"w"` vs `"a"` | `"w"` écrase, `"a"` ajoute à la suite |
