from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os, boto3
import config as c

# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \


sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
aws_credentials = c.AWS_CREDENTIALS()
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_credentials["AWS_ACCESS_KEY"])
hadoopConf.set('fs.s3a.secret.key', aws_credentials["AWS_SECRET_KEY"])
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

#s3 = boto3.resource("s3").Bucket()

# Read from the S3 bucket
df = spark.read.json("s3a://pinterest-data-0759ba42-ccf0-4396-9b86-76de3b8e6640/raw_data/year=2022/month=11/day=14/*.json") # You may want to change this to read csv depending on the files your reading from the bucket
df.show()


'''findspark.init()

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
sc = session.sparkContext.parallelize([1,2,3,4,5])
print(sc.take(3))
power_of_self = sc.map(lambda x: x**x)
print(power_of_self.take(5))

result = (
    sc.parallelize(dataC)
    .filter(lambda val)
)'''