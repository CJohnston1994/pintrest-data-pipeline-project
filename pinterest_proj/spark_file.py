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

    def null_if_not_match(self, value:str, comparison:str):
        '''
        Trys to match a value to a comparison
        if the value does not match the comparison it is returned
        if the value matches the comparison the return will be None type
        '''
        return when(value != comparison, value).otherwise(lit(None))

    def read_from_s3(self, date =(datetime.now()-timedelta(days = 1))):
        '''
        Function atakes in a date time and scrapes the data from that day,
        if no date is given it takes the current date and scrapes the previous day
        The path a constant taken from the env file and the date given is used to complete the S3 cluster path
        '''
        # Read from the S3 bucket
        
        return self.spark.read.json(f"{c.BUCKET_PATH}/year={date.year}/month={date.month}/day={str(date.day).zfill(2)}/*.json")       

    def clean_data(self, df:DataFrame):
        '''
        firstly a dict of values per row that need to be cleaned is set,
        the duplicates and NA values are dropped from the selected data
        follower_count suffix repleced with relevant number of 0's
        the dict is userd to clean the other balues
        '''
        original_order = "unique_id", "title", "description", "follower_count", "tag_list", "is_image_or_video", "image_src", "downloaded", "save_location", "category"
        print(df.schema)

        to_null =  {"title" : "No Title Data Available",
                    "description":"No description available Story format",
                    "follower_count" : "User Info Error",
                    "tag_list" : "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
                    "image_src" : "Image src error."
                    }

        #drop na and duplicate rows
        df = df.dropDuplicates(subset=["unique_id", "title", "description", "tag_list"]) \
            .dropna(thresh=2, subset=["title", "description", "tag_list"])

        #convert follower count from suffixed string to integer
        df= df.withColumn("follower_count", regexp_replace("follower_count", "k", "000")) \
              .withColumn("follower_count", regexp_replace("follower_count", "M", "000000")) \
              .withColumn("follower_count", regexp_replace("follower_count", "B", "000000000"))
        # Replace all values in df that match the to_null dict values

        for key, value in to_null.items():
            df = df.withColumn(key, self.null_if_not_match(col(key), value))

        #clean save locations string    
        df = df.withColumn("save_location", regexp_replace(col("save_location"), "Local save in", ""))

        #cast ffollower type to int
        df = df.withColumn("follower_count", df["follower_count"].cast(types.IntegerType()))

        #cast downloaded to bool
        df = df.withColumn("downloaded", df["downloaded"].cast(types.BooleanType()))

        df = df.select("unique_id", "title", "description", "follower_count", "tag_list", "is_image_or_video", "image_src", "downloaded", "save_location", "category")

        return df

if __name__ == "__main__":
    sbc = Spark_batch_controller()
    df = sbc.read_from_s3(datetime(2022,11,7))
    clean_df = sbc.clean_data(df)
    clean_df.show()