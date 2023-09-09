#==============================================================================#
#                                                                              #
#    Title: Title                                                              #
#    Comment: This is Chris's attempt to try and read the schema directly from #
#             Kafka, instead of writing it all out myself.                     #
#             You know, coz I'm lazy.                                          #
#    Sources:                                                                  #
#    - https://keestalkstech.com/2019/11/kafka-spark-and-schema-inference/     #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Import Globals                                                            ####
#------------------------------------------------------------------------------#

import sys, os, json, re
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


#------------------------------------------------------------------------------#
# Fix local issue                                                           ####
#------------------------------------------------------------------------------#

# Ensure the directory is correct... Every time. ----
for i in range(5):
    if not os.path.basename(os.getcwd()).lower() == "mdsi_bde_aut21_at3":
        os.chdir("..")
    else:
        break

# Ensure the current directory is in the system path. ---
if not os.path.abspath(".") in sys.path: sys.path.append(os.path.abspath("."))


#------------------------------------------------------------------------------#
# Local Imports                                                             ####
#------------------------------------------------------------------------------#

from src.secrets import secrets as sc



#------------------------------------------------------------------------------#
#                                                                              #
#    Functions                                                              ####
#                                                                              #
#------------------------------------------------------------------------------#


def read_kafka_topic \
    ( spark_session:SparkSession
    , kafka_topic:str
    , kafka_broker:str
    ):
    
    # Load Data
    df_json = spark_session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
        
    # Filter out empty values
    df_json = df_json \
        .withColumn("value", F.expr("string(value)")) \
        .filter(F.col("value").isNotNull())
    
    # Get the latest version of each record
    df_json = df_json \
        .select("key", F.expr("struct(offset, value) r")) \
        .groupBy("key") \
        .agg(F.expr("max(r) r")) \
        .select("r.value")
    
    # decode the json values
    df_read = spark_session \
        .read \
        .json \
            ( df_json.rdd.map(lambda x: x.value)
            , multiLine=True
            )
    
    # drop corrupt records
    if "_corrupt_record" in df_read.columns:
        df_read = df_read \
            .filter(F.col("_corrupt_record").isNotNull()) \
            .drop("_corrupt_record")
    
    # Return
    return df_read


def prettify_spark_schema_json(json_schema:str):
    
    parsed = json.loads(json_schema)
    raw = json.dumps(parsed, indent=1, sort_keys=False)

    str1 = raw

    # replace empty meta data
    str1 = re.sub('"metadata": {},\n +', '', str1)

    # replace enters between properties
    str1 = re.sub('",\n +"', '", "', str1)
    str1 = re.sub('e,\n +"', 'e, "', str1)

    # replace endings and beginnings of simple objects
    str1 = re.sub('"\n +},', '" },', str1)
    str1 = re.sub('{\n +"', '{ "', str1)

    # replace end of complex objects
    str1 = re.sub('"\n +}', '" }', str1)
    str1 = re.sub('e\n +}', 'e }', str1)

    # introduce the meta data on a different place
    str1 = re.sub('(, "type": "[^"]+")', '\\1, "metadata": {}', str1)
    str1 = re.sub('(, "type": {)', ', "metadata": {}\\1', str1)

    # make sure nested ending is not on a single line
    str1 = re.sub('}\n\s+},', '} },', str1)

    # Return
    return str1



#------------------------------------------------------------------------------#
#                                                                              #
#    Set main                                                               ####
#                                                                              #
#------------------------------------------------------------------------------#


def main():

    # Kafka
    kafka_broker = "localhost:9092"
    kafka_topic = "twitterdata"
    
    # Set Spark
    spark = SparkSession \
        .builder \
        .appName("kafka") \
        .getOrCreate()
        
    # Get DF
    df = read_kafka_topic \
        ( spark_session=spark
        , kafka_topic=kafka_topic
        , kafka_broker=kafka_broker
        )
        
    # Get pretty schema
    json_schema = prettify_spark_schema_json(df.schema.json())
    
    # See what you've got
    print(json_schema)
    
    # Return
    return None
    


#------------------------------------------------------------------------------#
#                                                                              #
#    Do Work                                                                ####
#                                                                              #
#------------------------------------------------------------------------------#


if __name__=='__main__':
    main()