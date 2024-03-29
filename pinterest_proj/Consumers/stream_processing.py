from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType 
import os

'''
This file consumes data from the Kafka topic and transforms it in real time
using the spark structured streaming functionality.
'''

# setup packages if neededd 
os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.2.10 pyspark-shell'

#kafka variables
kafka_topic = "PintrestData"
kafka_bootstrap_servers = "localhost:9092"

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName(kafka_topic) \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("ERROR")

# Read stream from Kafka topic
stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()

def null_if_match(value:str, comparison:str):
        '''
        compares two strings and returns a None value if they match
        '''
        return value if value != comparison else None

# Define the Schema to convert kafka data to a JSON object
jsonSchema = StructType([StructField("index", IntegerType()),
                         StructField("unique_id", StringType()),
                         StructField("title", StringType()),
                         StructField("description", StringType()),
                         StructField("follower_count", StringType()),
                         StructField("tag_list", StringType()),
                         StructField("is_image_or_video", StringType()),
                         StructField("image_src", StringType()),
                         StructField("downloaded", IntegerType()),
                         StructField("save_location", StringType()),
                         StructField("category", StringType())
                         ])

to_null =  {"title" : "No Title Data Available",
            "description":"No description available Story format",
            "follower_count" : "User Info Error",
            "tag_list" : "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
            "image_src" : "Image src error."}

#cast kafka stream to string
stream_df = stream_df.selectExpr("CAST(value as STRING)") \

# Apply schema to stream data
stream_df = stream_df.withColumn("value", from_json(stream_df["value"], jsonSchema)) \
            .select(col("value.*"))

#use to_null dict to change dirty data to None type
stream_df = [stream_df.withColumn(key, null_if_match(col(key), value))for key, value in to_null.items()]

# Stream Cleaning
# Drop duplicates and NA values
# Clean follower count
# Replace all values in df that match the to_null dict values
# Cast follower type to int
# Clean save location and is_image_or_video strings    
# Cast downloaded to bool
stream_df = stream_df.dropDuplicates(subset=["unique_id", "title", "description", "tag_list"]) \
                     .dropna(thresh=2, subset=["title", "description", "tag_list"]) \
                     .withColumn("follower_count", regexp_replace("follower_count", "k", "000")) \
                     .withColumn("follower_count", regexp_replace("follower_count", "M", "000000")) \
                     .withColumn("follower_count", regexp_replace("follower_count", "B", "000000000")) \
                     .withColumn("follower_count", stream_df["follower_count"].cast(types.IntegerType())) \
                     .withColumn("save_location", regexp_replace(col("save_location"), "Local save in ", "")) \
                     .withColumn("is_image_or_video", regexp_replace(col("is_image_or_video"), "(story page format)", "")) \
                     .withColumn("downloaded", stream_df["downloaded"].cast(types.BooleanType()))

stream_df = stream_df.select(["unique_id",
                              "title",
                              "description",
                              "follower_count",
                              "tag_list",
                              "is_image_or_video",
                              "image_src",
                              "downloaded",
                              "save_location",
                              "category"])

def foreach_batch_function(df, epoch_id):
        df.write.mode("append") \
         .format("jdbc") \
         .option("url", "jdbc:postgresql://localhost:5432/pinterest_streaming") \
         .option("driver", "org.postgresql.Driver") \
         .option("dbtable", "experimental_data") \
         .option("user", "postgres") \
         .option("password", "123") \
         .save()

stream_df.writeStream.foreachBatch(foreach_batch_function) \
         .start().awaitTermination()