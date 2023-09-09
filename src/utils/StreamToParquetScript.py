# Import necassary libraries:
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, BooleanType
from IPython.display import display, clear_output
import time

# Setup connection and subscribed to the twitterdata topic:
# Open spark session
spark = SparkSession.builder \
        .appName('kafka') \
        .getOrCreate()


# Subscribed to twitterdata topic
stream_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:29092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe", "twitterdata") \
  .load()

# Convert the value column to string, then convert the JSON string to StructType *(online including the columns we are interested in)*:
string_stream_df = stream_df.withColumn("value", stream_df["value"].cast(StringType()))
tweet_schema = StructType([
    StructField('created_at', StringType(), True),
    StructField('id', LongType(), True),
    StructField('text', StringType(), True),
    StructField('is_quote_status', BooleanType(), True),
    StructField('in_reply_to_user_id', LongType(), True),
    StructField('user', StructType([
        StructField('id', LongType(), True),
        StructField('followers_count', IntegerType(), True),
        StructField('friends_count', IntegerType(), True),
        StructField('created_at', StringType(), True)
    ])),
    StructField('extended_tweet', StructType([
        StructField('full_text', StringType(), True)
    ])),
    StructField('retweeted_status', StructType([
        StructField('id', LongType(), True)
    ])),
    StructField('retweet_count', IntegerType(), True),
    StructField('favorite_count', IntegerType(), True),
    StructField('quote_count', IntegerType(), True),
    StructField('reply_count', IntegerType(), True)
])
struct_stream_df = string_stream_df.withColumn("value", F.from_json("value", tweet_schema))

# Select the columns we want to persist to Parquet:

twitter_flat_df = struct_stream_df.select(
    'value.created_at'
    , 'value.id'
    , 'value.text'
    , 'value.is_quote_status'
    , 'value.in_reply_to_user_id'
    , F.col('value.user.id').alias('user_id')
    , F.col('value.user.followers_count').alias('user_followers_count')
    , F.col('value.user.friends_count').alias('user_friends_count')
    , F.col('value.user.created_at').alias('user_created_at')
    , F.col('value.extended_tweet.full_text').alias('extended_full_text')
    , F.col('value.retweeted_status.id').alias('retweeted_status_id')
    , 'value.retweet_count'
    , 'value.favorite_count'
    , 'value.quote_count'
    , 'value.reply_count'
).withColumn('is_retweet', F.isnull(F.col('retweeted_status_id')) != True)

# Start streaming into Parquet *(note the below cell will continue running until to manually stop it)*:
flat_stream_out = twitter_flat_df \
    .writeStream \
    .outputMode("append") \
    .option('path', 'data/twitter_flat.parquet') \
    .option("checkpointLocation", "data/twitter_flat.checkpoint") \
    .start()
flat_stream_out.awaitTermination()