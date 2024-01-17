
from time import sleep

from json import dumps
from json import loads





import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
#from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
#from pyspark.ml.feature import Tokenizer, RegexTokenizer
import re
from textblob import TextBlob

spark = SparkSession\
        .builder\
        .appName("RedditSentimentAnalysis")\
        .getOrCreate()


schema = StructType() \
    .add("submission_id", StringType()) \
    .add("comment_id", StringType()) \
    .add("author", StringType()) \
    .add("body", StringType())

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

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    tweet = re.sub(r'@[A-Za-z0–9]+', '', str(tweet)) #Remove @mentions replace with blank
    tweet = re.sub(r'#', '', str(tweet)) #Remove the ‘#’ symbol, replace with blank
    tweet = re.sub(r'RT[\s]+', '', str(tweet)) #Removing RT, replace with blank
    tweet = re.sub(r'https?:\/\/\S+', '', str(tweet)) #Remove the hyperlinks
    tweet = re.sub(r':', '', str(tweet)) # Remove :
    tweet = re.sub(r'\\','',str(tweet))
    tweet = re.sub(r'\>','',str(tweet))
    tweet = re.sub(r'"','',str(tweet))
    tweet = re.sub(r'“','',str(tweet))
    

    return tweet


#Next we have to remove emoji & Unicode from the Tweet data.
def remove_emoji(string):
 emoji_pattern = re.compile('['
 u"\U0001F600-\U0001F64F" # emoticons
 u"\U0001F300-\U0001F5FF" # symbols & pictographs
 u"\U0001F680-\U0001F6FF" # transport & map symbols
 u"\U0001F1E0-\U0001F1FF" # flags (iOS)
 u"\U00002500-\U00002BEF" # chinese char
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
 u"\ufe0f" # dingbats
 u"\u3030"
 "]+", flags=re.UNICODE)
 return emoji_pattern.sub(r'', string)

clean_tweets = F.udf(cleanTweet, StringType())
raw = df.withColumn('pre_processed_text', clean_tweets(col("body")))
comments = raw.withColumn('processed_text', clean_tweets(col("pre_processed_text")))


# Create a function to get the subjectifvity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'
    

subjectivity = F.udf(getSubjectivity, FloatType())
polarity = F.udf(getPolarity, FloatType())
sentiment = F.udf(getSentiment, StringType())

subjectivity_tweets = comments.withColumn('subjectivity', subjectivity(col("processed_text")))
polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))

# Écrire les données de streaming dans une table en mémoire
query = sentiment_tweets \
    .writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("sentiment") \
    .start()

# Interroger et afficher les résultats périodiquement
import time
for x in range(5):  # Par exemple, exécuter 10 fois
    spark.sql("SELECT author,processed_text,subjectivity,sentiment FROM sentiment").show()
    time.sleep(5)  # Attendre 5 secondes entre chaque requête

print("hello ariel")