import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime
from confluent_kafka import Producer

# initialize env variables
load_dotenv()

# list of African cities
cities = [
    "Nairobi",
    "Kampala",
    "Mogadishu",
    "Dodoma",
    "Kigali"
]

WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')

# kafka configuration
kafka_config = {
    "bootstrap.servers":os.getenv('BOOTSTRAP_SERVERS'),
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv('KAFKA_API_KEY'),
    "sasl.password": os.getenv('KAFKA_API_SECRET')    
}

# initialize kafka producer 
producer = Producer(kafka_config)

# topic name 
topic = "mawingu"

# delivery report callback 
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}3")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# loop through the cities list 
for city in cities:
    weather_url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={WEATHER_API_KEY}'
    response = requests.get(weather_url)
    data = response.json()

    if response.status_code == 200:
        print(f"Weather for {city}:")
        # converts json into string
        print(json.dumps(data, indent=2))
    else:
        print(f"Failed for {city}: {response.status_code}")