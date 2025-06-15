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
consumer.subscribe(['mawingu'])

print("Consuming from the topic...")


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error", msg.error())
            continue
        
        value = msg.value().decode('utf-8')
        key = msg.key().decode('utf-8') if msg.key() else "null"
        
        print(f"Consumed weather for {key}:")
        print(json.dumps(json.loads(value), indent=2))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()