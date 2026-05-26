FROM python:3.11-slim

WORKDIR /app

# Installation de cron (absent de python:3.11-slim par défaut)
RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python en premier (layer mis en cache si requirements.txt ne change pas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code du scraper
COPY scraper.py .

# Copier et activer le crontab (définit la tâche planifiée)
COPY crontab /etc/cron.d/books-cron
RUN chmod 0644 /etc/cron.d/books-cron

# Copier et rendre exécutable le script de démarrage
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Point d'entrée : lance cron + tail -f sur les logs (visible dans docker logs)
CMD ["/entrypoint.sh"]
