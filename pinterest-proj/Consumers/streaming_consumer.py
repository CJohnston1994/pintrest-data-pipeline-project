from kafka import KafkaConsumer
import json

streaming_consumer = KafkaConsumer(
    "PintrestData",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda  x: json.loads(x.decode("utf-8"))  
)



for message in streaming_consumer:
    print(message)