# Scraper books.toscrape.com

Pipeline de scraping qui collecte les 1 000 livres du site en moins de 11 secondes et les exporte en CSV.

## Lancer avec Docker

```bash
./run.sh
```

Le container démarre, cron scrape le site entier toutes les minutes et écrit dans `data/books.csv`.

```bash
docker logs -f session3_scraper   # logs en temps réel
```

## Lancer sans Docker

```bash
pip install -r requirements.txt
python scraper.py --max-pages 0 --workers 50 --output data/books.csv
```

## Options

| Option | Défaut | Description |
|--------|--------|-------------|
| `--output` | `data/books.csv` | Chemin du fichier CSV |
| `--max-pages` | `0` | Nb de pages catalogue (0 = tout le site, 50 pages) |
| `--workers` | `50` | Threads parallèles |

## Comment ça fonctionne

**Phase 1 — Catalogue (50 requêtes en parallèle)**
Les URLs des 50 pages catalogue sont prévisibles (`page-1.html` → `page-50.html`), donc téléchargées simultanément. Durée : ~1.6s.

**Phase 2 — Fiches livres (1 000 requêtes en parallèle)**
Les 1 000 URLs collectées en Phase 1 sont toutes soumises d'un coup au pool de workers. Durée : ~8.6s.

**Total : 1 050 requêtes HTTP en < 11 secondes.**

## Structure des fichiers

```
session3/
├── scraper.py          # Pipeline principal (2 phases parallèles)
├── Dockerfile          # Image Python 3.11 + cron
├── docker-compose.yaml # Monte ./data:/data pour persister le CSV
├── entrypoint.sh       # Lance cron + tail -f sur les logs
├── crontab             # Exécution toutes les minutes
├── requirements.txt    # requests, beautifulsoup4
├── run.sh              # docker compose up --build -d
└── data/
    └── books.csv       # Généré automatiquement
```

## Format du CSV

```
title,price,rating,category,date
A Light in the Attic,£51.77,3,Poetry,2026-05-19
Tipping the Velvet,£53.74,1,Historical Fiction,2026-05-19
...
```

Le fichier est réinitialisé à chaque run (pas d'accumulation).
