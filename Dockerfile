FROM quay.io/astronomer/astro-runtime:12.7.1

# Définir le répertoire de travail (le répertoire racine du projet ici)
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt /app/requirements.txt

# Installer les dépendances à partir du fichier requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copier l'ensemble du projet (dags et autres fichiers) dans le conteneur
COPY . /app

USER root

# S'assurer que le fichier estate.log existe et a les bonnes permissions
RUN chown astro:astro /app/estate.log && \
    chmod 666 /app/estate.log

# Revenir à l'utilisateur astro pour les étapes suivantes
USER astro