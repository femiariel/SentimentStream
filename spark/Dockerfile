FROM bitnami/spark:latest

# Définissez le répertoire de travail dans le conteneur
WORKDIR /app
 
# Copiez uniquement le fichier requirements.txt dans le conteneur pour éviter de cacher inutilement les couches du Docker cache
COPY requirementsspark.txt /app/
COPY str.py  /opt/bitnami/spark/work/

# Installez les dépendances Python
RUN pip install --no-cache-dir -r requirementsspark.txt