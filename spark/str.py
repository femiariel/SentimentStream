from time import sleep
from json import dumps, loads
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.types import StringType, StructType, FloatType
from pyspark.sql import SparkSession
import re
from textblob import TextBlob

# Initialiser une session Spark
spark = SparkSession\
        .builder\
        .appName("RedditSentimentAnalysis")\
        .getOrCreate()

# Définir le schéma des données entrantes
schema = StructType() \
    .add("submission_id", StringType()) \
    .add("comment_id", StringType()) \
    .add("author", StringType()) \
    .add("body", StringType())

# Lire les données de streaming à partir de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reddit_com") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Fonction pour nettoyer les commentaires
def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # Remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove punctuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # Remove numbers
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # Remove hashtags
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    tweet = re.sub(r'@[A-Za-z0–9]+', '', str(tweet))  # Remove @mentions
    tweet = re.sub(r'#', '', str(tweet))  # Remove the ‘#’ symbol
    tweet = re.sub(r'RT[\s]+', '', str(tweet))  # Removing RT
    tweet = re.sub(r'https?:\/\/\S+', '', str(tweet))  # Remove hyperlinks
    tweet = re.sub(r':', '', str(tweet))  # Remove :
    tweet = re.sub(r'\\','',str(tweet))
    tweet = re.sub(r'\>','',str(tweet))
    tweet = re.sub(r'"','',str(tweet))
    tweet = re.sub(r'“','',str(tweet))

    return tweet

# Fonction pour supprimer les emojis et Unicode
def remove_emoji(string):
    emoji_pattern = re.compile('['
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

# Définir les UDFs pour nettoyer les tweets
clean_tweets = F.udf(cleanTweet, StringType())
raw = df.withColumn('pre_processed_text', clean_tweets(col("body")))
comments = raw.withColumn('processed_text', clean_tweets(col("pre_processed_text")))

# Fonction pour obtenir la subjectivité
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity

# Fonction pour obtenir la polarité
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

# Fonction pour obtenir le sentiment à partir de la polarité
def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'

# Définir les UDFs pour la subjectivité, polarité et sentiment
subjectivity = F.udf(getSubjectivity, FloatType())
polarity = F.udf(getPolarity, FloatType())
sentiment = F.udf(getSentiment, StringType())

# Appliquer les transformations pour obtenir la subjectivité, polarité et sentiment
subjectivity_tweets = comments.withColumn('subjectivity', subjectivity(col("processed_text")))
polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))

# Écrire les données de streaming dans un fichier parquet
query = sentiment_tweets \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/bitnami/spark/work/sentiment_data") \
    .option("checkpointLocation", "/opt/bitnami/spark/work/checkpoint") \
    .start()

# Rester en attente pour que le streaming continue
query.awaitTermination()

"""
# Interroger et afficher les résultats périodiquement
import time
for x in range(5):  # Par exemple, exécuter 10 fois
    d=spark.sql("SELECT author,processed_text,subjectivity,sentiment FROM sentiment").toPandas()
    print(d)
    time.sleep(5)  # Attendre 5 secondes entre chaque requête

print("hello ariel")
"""
