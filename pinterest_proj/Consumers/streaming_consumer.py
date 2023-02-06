from pyspark.sql import SparkSession, types
import pyspark.sql.functions as PysparkSQLFunctions
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType 
import os

'''
This file consumes data from the Kafka topic and transforms it in real time
using the spark structured streaming functionality.
'''

# setup packages if neededd 
os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell'

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

def null_if_not_match(value:str, comparison:str):
        '''
        compares two strings, value and comparison
        if value does not match comparison, value is returned
        if value matches comparison the return will be None type
        '''
        return PysparkSQLFunctions.when(value != comparison, value).otherwise(PysparkSQLFunctions.lit(None))

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
stream_df = stream_df.selectExpr("CAST(value as STRING)")

# Apply schema to stream data
stream_df = stream_df.withColumn("value", PysparkSQLFunctions \
            .from_json(stream_df["value"], jsonSchema)) \
            .select(col("value.*"))

# Stream Cleaning
# Drop duplicates and NA values
stream_df = stream_df.dropDuplicates(subset=["unique_id", "title", "description", "tag_list"]) \
                     .dropna(thresh=2, subset=["title", "description", "tag_list"])

# Clean follower count
stream_df = stream_df.withColumn("follower_count", regexp_replace("follower_count", "k", "000")) \
                     .withColumn("follower_count", regexp_replace("follower_count", "M", "000000")) \
                     .withColumn("follower_count", regexp_replace("follower_count", "B", "000000000")) 

#cast follower type to int
stream_df = stream_df.withColumn("follower_count", stream_df["follower_count"].cast(types.IntegerType()))

# Replace all values in df that match the to_null dict values
for key, value in to_null.items():
    stream_df = stream_df.withColumn(key, null_if_not_match(col(key), value))

#clean save locations string    
stream_df = stream_df.withColumn("save_location", regexp_replace(col("save_location"), "Local save in", ""))

#cast downloaded to bool
stream_df = stream_df.withColumn("downloaded", stream_df["downloaded"].cast(types.BooleanType()))

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

# Stream output
query = stream_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .start() \
        .awaitTermination()