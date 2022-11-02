from kafka import KafkaConsumer
import json

batch_consumer = KafkaConsumer(
    "PintrestData",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda  x: json.loads(x.decode("utf-8"))  
)


