# Projet d'Analyse de Sentiments en Temps Réel des Commentaires Reddit

## Description
Ce projet vise à analyser les sentiments des commentaires sur Reddit sur le thème de la finance en temps réel en utilisant un pipeline de traitement de données comprenant Apache Kafka, Zookeeper, et Spark Streaming. Le projet est construit en Python et est déployé à l'aide de Docker. Le dashboard sur l'interface graphique est actualisé chaque 10 min avec les données en temps réel .

## Prérequis
- Docker et Docker Compose doivent être installés sur votre système.
- Une compréhension de base de Kafka, Spark Streaming, et Python est recommandée.

## Architecture
Le projet utilise les composants suivants :
- **Apache Kafka** : pour la gestion des flux de données en temps réel.
- **Zookeeper** : pour la coordination et la gestion des nœuds Kafka.
- **Spark Streaming** : pour le traitement en temps réel des données.
- **Python** : pour les scripts d'analyse de sentiments et l'interaction avec l'API Reddit.
- **Docker** : pour la conteneurisation et la simplification du déploiement.
- **Dash** : pour l'interface graphique

## Configuration et Installation
Assurez-vous que Docker et Docker Compose sont installés sur votre machine. Clonez ensuite ce dépôt sur votre machine locale.

## Lancement du Projet
Pour démarrer le projet, exécutez les commandes suivantes dans le répertoire racine du projet :

1. Construisez les images Docker :
   ```bash
   docker-compose build
   ```
2. Lancez les conteneurs :
   ```bash
   docker-compose up
   ```
3. Accéder à l'interface graphique en lançant le lien suivant :
   ```
   http://localhost:8050
   ```
Ces commandes construiront et lanceront les conteneurs nécessaires pour Apache Kafka, Zookeeper, et Spark Streaming, ainsi que pour l'application Python.

## Fonctionnement
Une fois les conteneurs lancés, le pipeline de traitement des données est activé. Les commentaires Reddit sont récupérés en temps réel, envoyés à Kafka, puis traités par Spark Streaming pour l'analyse de sentiments. Les résultats peuvent ensuite etre visualisés via l'interface graphique sur le lien donné au point 3 . Chaque 1O min, le dashboard s'actualise avec les données en temps réel .



