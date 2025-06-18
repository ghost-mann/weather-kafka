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
    

if response.status_code == 200:
        data = response.json()
        try:
            forecast = data['list'][0]  # Get the first forecast entry
            weather_payload = {
                "city": city,
                "timestamp": forecast['dt_txt'],  # e.g. '2025-06-17 18:00:00'
                "temperature": forecast['main']['temp'],  # e.g. 25.3
                "description": forecast['weather'][0]['description']  # e.g. 'light rain'
            }

            producer.produce(
                topic=topic,
                key=city,
                value=json.dumps(weather_payload),
                callback=delivery_report
            )
            producer.poll(0)

        except KeyError as e:
            print(f"Malformed response for {city}: {e}")
else:
        print(f"Failed for {city}: {response.status_code}")

producer.flush()