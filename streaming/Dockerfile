# Utilisez l'image de base officielle Python 3.6
FROM python:3.6
 
# Ajouter la dépendance Kafka pour Spark
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar


# Définissez le répertoire de travail dans le conteneur
WORKDIR /app
 
# Copiez uniquement le fichier requirements.txt dans le conteneur pour éviter de cacher inutilement les couches du Docker cache
COPY requirements.txt /app/
 
# Installez les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt
 
# Copiez le reste des fichiers de l'application dans le conteneur
COPY . /app
 
 
# Définissez la commande par défaut pour exécuter l'application Flask
CMD ["python", "redd.py"]