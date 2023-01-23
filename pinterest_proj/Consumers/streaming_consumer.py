from pyspark.sql import SparkSession
import pyspark.sql.functions as PysparkSQLFunctions
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

# Define the Schema to convert kafka data to a JSON object
jsonSchema = StructType([StructField("index", IntegerType()),
                        StructField("unique_id", StringType()),
                        StructField("title", StringType()),
                        StructField("description", StringType()),
                        StructField("poster_name", StringType()),
                        StructField("follower_count", StringType()),
                        StructField("is_image_or_video", StructType()),
                        StructField("image_src", StringType()),
                        StructField("downloaded", IntegerType()),
                        StructField("save_location", IntegerType()),
                        StructField("category", IntegerType())
                    ])
# Stream Cleaning
stream_df = stream_df.selectExpr("CAST(value as STRING)")

stream_df = stream_df \
            .withColumn("value", PysparkSQLFunctions \
            .from_json(stream_df["value"], jsonSchema)) \
            .select(col("value.*"))      

# Stream output
query = stream_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .start() \
        .awaitTermination()