from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
import json

load_dotenv()

consumer_config = {
    'bootstrap.servers':os.getenv("BOOTSTRAP_SERVERS"),
    'security_protocol': 'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':os.getenv('KAFKA_API_KEY'),
    'sasl.password':os.getenv('KAFKA_API_SECRET'),
    'group.id':'weather_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)