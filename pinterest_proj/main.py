import json, uuid
import logging as log
from kafka import KafkaConsumer

from API import project_pin_API as api

if __name__ == '__main__':
    my_uuid = uuid.uuid4()
    print(f"pinterest_data_{my_uuid}")