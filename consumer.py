from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
import json
# from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider

load_dotenv()

consumer_config = {
    'bootstrap.servers':os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':os.getenv('KAFKA_API_KEY'),
    'sasl.password':os.getenv('KAFKA_API_SECRET'),
    'group.id':'weather_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['mawingu'])
print("Consuming from the topic...")


# Cassandra config
# CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
# CASSANDRA_USER = os.getenv("CASSANDRA_USER", "")
# CASSANDRA_PASS = os.getenv("CASSANDRA_PASS", "")

# auth_provider = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASS)
# cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)
# session = cluster.connect()

# # Ensure keyspace and table exist
# session.execute("""
#     CREATE KEYSPACE IF NOT EXISTS weather_data 
#     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
# """)
# session.set_keyspace("weather_data")

# session.execute("""
#     CREATE TABLE IF NOT EXISTS weather (
#         city TEXT,
#         timestamp TEXT,
#         temperature FLOAT,
#         description TEXT,
#         PRIMARY KEY (city, timestamp)
#     )
# """)

# insert_stmt = session.prepare("""
#     INSERT INTO weather (city, timestamp, temperature, description)
#     VALUES (?, ?, ?, ?)
# """)

# print("Consuming from the topic and inserting into Cassandra...")


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