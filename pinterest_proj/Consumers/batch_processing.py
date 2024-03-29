from pyspark.sql import SparkSession, types, DataFrame
from pyspark.sql.functions import regexp_replace, when, lit, col
from pyspark import SparkContext, SparkConf
from datetime import datetime, timedelta
import os
import config as c


class SparkBatchController():
    def __init__(self):
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

    def null_if_not_match(self, value:str, comparison:str):
        '''
        compares two strings, value and comparison
        if value does not match comparison, value is returned
        if value matches comparison the return will be None type
        '''
        return when(value != comparison, value).otherwise(lit(None))

    def read_from_s3(self, date =(datetime.now()-timedelta(days = 1))):
        '''
        Scrapes the given date, yesterday by default and returns the contents of the bucket.

        params: date = datetime

        returns: list of json objects or none if empty
        '''
        # Read from the S3 bucket
        
        return self.spark.read.json(f"{c.BUCKET_PATH}/year={date.year}/month={date.month}/day={str(date.day).zfill(2)}/*.json")       

    def clean_data(self, df:DataFrame):
        '''
        Cleans data, drops duplicates and NA values. Follower count
        is converted to an int and to_null lists other valuues to be cleaned
        '''

        to_null =  {"title" : "No Title Data Available",
                    "description":"No description available Story format",
                    "follower_count" : "User Info Error",
                    "tag_list" : "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
                    "image_src" : "Image src error."
                    }

        # Replace all values in df that match the to_null dict values
        for key, value in to_null.items():
            df = df.withColumn(key, self.null_if_not_match(col(key), value))

        #drop na and duplicate rows
        df = df.dropDuplicates(subset=["unique_id", "title", "description", "tag_list"]) \
            .dropna(thresh=2, subset=["title", "description", "tag_list"]) \
            .withColumn("follower_count", regexp_replace("follower_count", "k", "000")) \
            .withColumn("follower_count", regexp_replace("follower_count", "M", "000000")) \
            .withColumn("follower_count", regexp_replace("follower_count", "B", "000000000")) \
            .withColumn("follower_count", df["follower_count"].cast(types.IntegerType())) \
            .withColumn("save_location", regexp_replace(col("save_location"), "Local save in", "")) \
            .withColumn("downloaded", df["downloaded"].cast(types.BooleanType()))

        df = df.select(["unique_id",
                        "title",
                        "description",
                        "follower_count",
                        "tag_list",
                        "is_image_or_video",
                        "image_src",
                        "downloaded",
                        "save_location",
                        "category"])
        df.show()

        return df

    def run_batch_cleaner(self):

        '''
        Run clean files from the s3 bucket and return them
        '''
        
        df = self.read_from_s3()

        return self.clean_data(df)


if __name__ == "__main__":
    sbc = SparkBatchController()
    clean_df = sbc.run_batch_cleaner()