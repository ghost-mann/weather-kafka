import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime

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

weather_url = f'http://api.openweathermap.org/data/2.5/forecast?id=524901&appid={WEATHER_API_KEY}'

response = requests.get(weather_url)

data = response.json()

if response.status_code == 200:
    print(data)
else:
    print("error")