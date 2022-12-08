from pyspark.sql import SparkSession, types, DataFrame
from pyspark.sql.functions import regexp_replace, when, lit, col
from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta
import os, boto3, pyspark
import config as c


class Spark_batch_controller():
    def __init__(self, yesterday = True):
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
        self.spark=SparkSession(sc)

    def null_if_not_match(self, value):
        return when(self != value, self).otherwise(lit(None))

    def read_from_s3(self, date = (datetime.now()-timedelta(days = 1))):
        # Read from the S3 bucket
        bucket_path = "s3a://pinterest-data-0759ba42-ccf0-4396-9b86-76de3b8e6640/raw_data"
        return self.spark.read.json(f"{bucket_path}/year={date.year}/month={date.month}/day={str(date.day).zfill(2)}/*.json")       

    def clean_data(self, df:DataFrame ):
        to_null =  {"title" : "No Title Data Available",
                    "description":"No description available Story format",
                    "follower_count" : "User Info Error",
                    "tag_list" : "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
                    "image_src" : "Image src error."
                    }

        #drop na and duplicate rows
        df = df.dropDuplicates(subset=["title", "description", "tag_list", "image_src"]) \
            .dropna(thresh=2, subset=["title", "description", "tag_list"])

        #convert follower count from suffixed string to integer
        df= df.withColumn("follower_count", regexp_replace("follower_count", "k", "000")) \
                  .withColumn("follower_count", regexp_replace("follower_count", "M", "000000")) \
                  .withColumn("follower_count", regexp_replace("follower_count", "B", "000000000"))
        # Replace all values in df that match the to_null dict values

        for key, value in to_null.items():
            df = df.withColumn(key, self.null_if_not_match(col(key), value))
        '''     
        df = df.withColumn("title", self.null_if_not_match(col("title"), to_null["title"]))
        df = df.withColumn("description", self.null_if_not_match(col("description"), to_null["description"])) \
        df = df.withColumn("follower_count", self.null_if_not_match(col("follower_count"), to_null["follower_count"])) \
        df = df.withColumn("tag_list", self.null_if_not_match(col("tag_list"), to_null["tag_list"])) \
        df = df.withColumn("image_src", self.null_if_not_match(col("image_src"), to_null["image_src"])) \
        '''
        df = df.withColumn("save_location", regexp_replace(col("save_location"), "Local save in", ""))

        df = df.withColumn("follower_count", df["follower_count"].cast(types.IntegerType()))

        return df

if __name__ == "__main__":
    sbc = Spark_batch_controller()
    df = sbc.read_from_s3(datetime(2022,11,7))
    clean_df = sbc.clean_data(df)
    clean_df.show()


'''
!!! - Before interview Go back to notebooks for theory

Schema = ('category', 'description', 'downloaded', 'follower_count', 'image_src', 'index', 'is_image_or_video', 'save_location', 'tag_list', 'title', 'unique_id')
Cleaning Data
Convert k and M in follower count to 0's
TODO - Title, Description, poster name, tag_list, img_src
'''