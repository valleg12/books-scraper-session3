# Pipeline de Ventes

Pipeline ETL qui traite des données de ventes brutes pour produire des rapports agrégés par région.

## Objectif

Transformer un fichier CSV de ventes brutes (avec données manquantes et invalides) en deux fichiers propres :
- les ventes nettoyées avec le chiffre d'affaires calculé
- un résumé agrégé par région

## Architecture

```
session3/
├── data/
│   ├── raw/
│   │   └── ventes.csv             # Données brutes en entrée
│   └── processed/
│       ├── ventes_nettoyees.csv   # Données nettoyées (output)
│       └── resume_par_region.csv  # Agrégat par région (output)
├── src/
│   ├── extract.py     # Lecture du fichier CSV
│   ├── transform.py   # Nettoyage, calcul CA, agrégation
│   └── load.py        # Sauvegarde des résultats
├── pipeline.py        # Point d'entrée principal
├── requirements.txt
├── Dockerfile
└── README.md
```

## Description des données

| Colonne         | Type    | Description                         |
|-----------------|---------|-------------------------------------|
| id              | int     | Identifiant unique de la commande   |
| date            | string  | Date de la vente (YYYY-MM-DD)       |
| region          | string  | Région de la vente                  |
| produit         | string  | Nom du produit vendu                |
| quantite        | float   | Quantité vendue (peut être vide)    |
| prix_unitaire   | float   | Prix unitaire (doit être > 0)       |
| client          | string  | Nom du client                       |

## Lancer le pipeline

### En local

```bash
pip install -r requirements.txt
python pipeline.py
```

### Avec Docker

```bash
docker build -t pipeline-ventes .
docker run pipeline-ventes
docker logs <container_id>
```

## Exemple de logs

```
2024-01-01 10:00:00 | INFO | ==================================================
2024-01-01 10:00:00 | INFO | PIPELINE DEMARRE
2024-01-01 10:00:00 | INFO | ==================================================
2024-01-01 10:00:00 | INFO | [EXTRACT] 20 lignes chargees en 0.01s
2024-01-01 10:00:00 | INFO | [TRANSFORM] 2 lignes supprimees | 18 lignes conservees (0.00s)
2024-01-01 10:00:00 | INFO | [TRANSFORM] Chiffre d'affaires total : 15849.58 euros
2024-01-01 10:00:00 | INFO | [TRANSFORM] 5 regions trouvees
2024-01-01 10:00:00 | INFO | [LOAD] 18 lignes sauvegardees en 0.00s
2024-01-01 10:00:00 | INFO | [LOAD] 5 lignes sauvegardees en 0.00s
2024-01-01 10:00:00 | INFO | PIPELINE TERMINE en 0.05s
```
