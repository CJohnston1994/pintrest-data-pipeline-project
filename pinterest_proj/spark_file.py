from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark import SparkContext, SparkConf
import os, boto3, pyspark
import config as c

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setMaster("local[*]") \
    .setAppName('Pinterest_spark_app') \


sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
aws_credentials = c.AWS_CREDENTIALS()
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_credentials["AWS_ACCESS_KEY"])
hadoopConf.set('fs.s3a.secret.key', aws_credentials["AWS_SECRET_KEY"])
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
df = spark.read.json("s3a://pinterest-data-0759ba42-ccf0-4396-9b86-76de3b8e6640/raw_data/year=2022/month=11/day=14/*.json") # You may want to change this to read csv depending on the files your reading from the bucket

'''
Cleaning Data
Convert k and M in follower count to 0's
TODO - Title, Description, poster name, tag_list, img_src
'''

df = df.withColumn("follower_count", regexp_replace(col("follower_count"), "k", "000")) \
    .withColumn("follower_count", regexp_replace(col("follower_count"), "M", "000000")) \
    .withColumn("follower_count", col("follower_count").cast("int")) \
    .show()
    

'''
correct = .withColumn('follower_count', regexp_replace(col('follower_count'), 'k', '000')) 
tried:
df.follower_count
col("follower_count")
$"follower_count"
df["follower_count"]

'''