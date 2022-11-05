from kafka import KafkaConsumer
import json, boto3

batch_consumer = KafkaConsumer(
    "PintrestData",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda  x: json.loads(x.decode("utf-8"))  
)

s3_session = boto3.session(aws_access_key_id=c.S3_ACCESS,aws_secret_access_key =c.S3_SECRET)