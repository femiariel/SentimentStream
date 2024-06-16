from kafka.producer import KafkaProducer
import praw
from concurrent.futures import ThreadPoolExecutor
from json import dumps, loads
from kafka import KafkaConsumer

# Initialisation de KafkaProducer pour envoyer des messages à Kafka
producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Configuration de PRAW (Python Reddit API Wrapper) pour accéder à Reddit
user_agent = "Ariel"
reddit = praw.Reddit(
    client_id="W2GIhHSqgb9K_XTph1twJw",
    client_secret="5aH6xUhSMOx92Sp66wEQv4vMsd4DJQ",
    user_agent=user_agent
)

# Nom du subreddit à analyser
subreddit_name = 'finance'
subreddit = reddit.subreddit(subreddit_name)

# Fonction pour récupérer les commentaires d'une soumission
def fetch_comments(submission):
    # Remplacer les "more comments" pour obtenir une liste complète de commentaires
    submission.comments.replace_more(limit=20)  # Limite ajustée pour les "more comments"
    for comment in submission.comments.list():
        comment_data = {
            'submission_id': comment.link_id,
            'comment_id': comment.id,
            'author': str(comment.author),
            'body': comment.body
        }
        # Envoyer les données de commentaire à Kafka
        producer.send('reddit_com', value=comment_data)

# Utilisation de ThreadPoolExecutor pour la récupération parallèle des commentaires
with ThreadPoolExecutor(max_workers=5) as executor:
    # Récupérer les soumissions les plus chaudes du subreddit
    for submission in subreddit.hot(limit=10):  # Limite ajustée pour les soumissions
        executor.submit(fetch_comments, submission)

# Fermer le producteur Kafka
producer.close()

# Configuration de KafkaConsumer pour consommer les messages du topic
topic_name = 'reddit_com'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("bravo")
