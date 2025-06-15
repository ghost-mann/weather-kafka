import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime
from confluent_kafka.admin import AdminClient

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

# create admin client
admin_client = AdminClient(kafka_config)


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