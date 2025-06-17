from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
import json
from cassandra.cluster import Cluster

load_dotenv()

consumer_config = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'weather_consumer_group',
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 30000,  # Just add this line
    'request.timeout.ms': 30000   # And this line
}

consumer = Consumer(consumer_config)
consumer.subscribe(['mawingu'])
print("Consuming from the topic...")

# Cassandra config
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

# Ensure keyspace and table exist
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS weather_data 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace("weather_data")

session.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        city TEXT,
        timestamp TEXT,
        temperature FLOAT,
        description TEXT,
        PRIMARY KEY (city, timestamp)
    )
""")

insert_stmt = session.prepare("""
    INSERT INTO weather (city, timestamp, temperature, description)
    VALUES (?, ?, ?, ?)
""")

print("Consuming from the topic and inserting into Cassandra...")

try:
    while True:
        msg = consumer.poll(5.0)  # Increased timeout from 1.0 to 5.0
        if msg is None:
            print("No message received, waiting...")  # Add this to see what's happening
            continue
        if msg.error():
            print("Consumer error", msg.error())
            continue
        
        value = msg.value().decode('utf-8')
        key = msg.key().decode('utf-8') if msg.key() else "null"
        
        if value:
            try:
                json_obj = json.loads(value)
                
                city = json_obj.get('city', 'unknown')
                timestamp = json_obj.get('timestamp', '')
                temp = float(json_obj.get('temperature', 0.0))
                desc = json_obj.get('description', '')
                
                session.execute(insert_stmt, (city, timestamp, temp, desc))
                
                print(f"Inserted weather for {city} at {timestamp}")
            except json.JSONDecodeError as e:
                print(f"Invalid JSON: {value}")
            except Exception as e:
                print(f"Error inserting into Cassandra: {e}")
        else:
            print(f"Received empty message for key '{key}'")
        
        print(f"Consumed weather for {key}:")
        print(json.dumps(json.loads(value), indent=2))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()