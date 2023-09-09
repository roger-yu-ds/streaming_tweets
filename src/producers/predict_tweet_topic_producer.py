from datetime import (
    datetime,
    timedelta
)
import pandas as pd
from pathlib import Path
from sklearn.datasets import fetch_20newsgroups
from bertopic import BERTopic
from joblib import (
    dump,
    load
)

from pyspark.sql import (
    SparkSession,
    functions as F
)
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    IntegerType,
    LongType,
    BooleanType,
    MapType
)
import sparknlp
from sparknlp import Finisher
from pyspark.ml import (
    Pipeline
)
from sparknlp.pretrained import PretrainedPipeline

from wordcloud import (
    WordCloud,
    STOPWORDS,
    ImageColorGenerator
)

from IPython.display import display, clear_output
import time


@F.udf
def predict_topic(text):
    prediction = topic_model.transform(text)
    topic_name = topic_model.get_topics()[prediction[0][0]][0][0]
    return topic_name


if __name__ == '__main__':
    spark = sparknlp.start()
    print(f'spark.version: {spark.version}')
    print(f'sparknlp.version(): {sparknlp.version()}')

    # Set up directories
    project_dir = Path.cwd().parents[1]
    models_dir = project_dir / 'models'
    pretrained_models_dir = models_dir / 'pretrained'
    data_dir = project_dir / 'data'
    raw_data_dir = data_dir / 'raw'
    processed_data_dir = data_dir / 'processed'

    path = models_dir / 'topic_model'
    if path.with_suffix('.joblib').exists():
        topic_model = load(path.with_suffix('.joblib'))

    stream_df = (
        spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("startingOffsets", "earliest")
            .option("subscribe", "twitterdata")
            #     .option("maxOffsetsPerTrigger",1)
            .load()
    )

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

    tweet_stream_df = (
        stream_df
            # Convert the key and value from binary to StringType
            .withColumn('key', stream_df['key'].cast(StringType()))
            .withColumn('value', stream_df['value'].cast(StringType()))
            # Assign fields to JSON
            .withColumn('value', F.from_json('value', tweet_schema))
            .select('timestamp',
                    'value.created_at',
                    'value.text',
                    'value.extended_tweet.full_text')
            .where(stream_df.timestamp > F.current_timestamp() - F.expr(
            'INTERVAL 1 seconds'))
    )

    spark.udf.register('predict_topic', predict_topic)

    kafka_checkpoint = processed_data_dir / 'kafka_checkpoint'

    while True:
        try:
            print('start streaming...')
            kafka_stream = (
                tweet_stream_df
                .select(F.col('text').alias('key'),
                        predict_topic('text').alias('value'))
                .where(tweet_stream_df.timestamp > (
                    F.current_timestamp() - F.expr('INTERVAL 1 seconds')))
                .writeStream
                .format('kafka')
                .option('checkpointLocation', kafka_checkpoint.as_posix())
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("topic", "topic_predictions")
                .start()
            )
        except BaseException as e:
            print("Error in stream.filter: %s, Pausing for 5 seconds..." % str(
                e))
            time.sleep(5)

