import json
import logging as log
from kafka import KafkaConsumer

from API import project_pin_API as api

consumer = KafkaConsumer(
    "PintrestConsumer",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))    )


if __name__ == '__main__':
    
    log.info("Data pipeline initiated")
    for message in consumer:
        print(message)