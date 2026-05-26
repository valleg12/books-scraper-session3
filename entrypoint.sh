#!/bin/sh
# entrypoint.sh — Script de démarrage du container
#
# Ce script fait 3 choses :
#   1. Crée le fichier de log s'il n'existe pas encore
#   2. Lance cron en arrière-plan (c'est lui qui exécute scraper.py toutes les minutes)
#   3. Fait un "tail -f" sur le fichier de log → redirige les logs vers stdout
#      ce qui les rend visibles dans "docker logs session3_scraper"

# Créer le dossier data et le fichier de log si besoin
mkdir -p /data
touch /data/scraper.log

echo "$(date '+%Y-%m-%d %H:%M:%S') | INFO     | [DOCKER] Container demarre — cron sera actif dans moins d'une minute"

# Démarrer le service cron en arrière-plan
cron

# Garder le container vivant en suivant le fichier de log en temps réel
# tail -f = "follow" : affiche les nouvelles lignes au fur et à mesure
# Tout ce que cron écrit dans /data/scraper.log apparaît aussi dans docker logs
tail -f /data/scraper.log
