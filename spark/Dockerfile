FROM bitnami/spark:latest

# Définissez le répertoire de travail dans le conteneur
WORKDIR /app
 
# Copiez uniquement le fichier requirements.txt dans le conteneur pour éviter de cacher inutilement les couches du Docker cache
COPY requirementsspark.txt /app/
COPY str.py  /opt/bitnami/spark/work/
COPY dash_server.py /opt/bitnami/spark/work/

# Créer les répertoires pour les données de sentiment et les points de contrôle
RUN mkdir -p /opt/bitnami/spark/work/sentiment_data
RUN mkdir -p /opt/bitnami/spark/work/checkpoint

# Installez les dépendances Python
RUN pip install --no-cache-dir -r requirementsspark.txt


# Exposer le port sur lequel l'application va fonctionner
EXPOSE 8050