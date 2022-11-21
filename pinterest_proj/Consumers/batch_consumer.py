from kafka import KafkaConsumer
from datetime import datetime
import pinterest_proj.config as c
import json, boto3, os
from pprint import pprint

batch_consumer = KafkaConsumer(
    "PintrestData",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda  x: json.loads(x.decode("utf-8"))
    )

s3_resource = boto3.resource('s3')
date = datetime.now().strftime("%Y-%m-%d").split('-')

for index, message in enumerate(batch_consumer):
    unique_id = message.value['unique_id'].replace('-', '_')
    path = os.path.join('raw_data', f'year={date[0]}', f'month={date[1]}', f'day={date[2]}', f'{unique_id}.json')
    s3_resource.Bucket(c.BUCKET_NAME).put_object(Body=json.dumps(message.value), Key=path)
    if index >= 100:
        break