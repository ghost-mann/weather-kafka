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

