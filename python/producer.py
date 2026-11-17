import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Configuration
load_dotenv()
API_KEY = os.getenv("API_KEY")
BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("KAFKA_TOPIC")

if not API_KEY or API_KEY == "xxx":
    raise ValueError("API_KEY is not set. Please set it in the .env file.")
if not BROKER or BROKER == "xxx":
    raise ValueError("KAFKA_BROKER is not set. Please set it in the .env file.")
if not TOPIC or TOPIC == "xxx":
    raise ValueError("KAFKA_TOPIC is not set. Please set it in the .env file.")

API_URL = "https://api.football-data.org/v4/competitions/PD/matches"
HEADERS = {'X-Auth-Token': API_KEY}

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

print("Starting Football API Producer...")

while True:
    try:
        # 1. Fetch data from API
        response = requests.get(API_URL, headers=HEADERS)
        response.raise_for_status()
        matches = response.json().get('matches', [])
        
        print(f"Fetched {len(matches)} matches.")

        # 2. Send each match as a separate event to Kafka
        for match in matches:
            match_id = match.get('id', 'unknown')
            producer.send(TOPIC, key=match_id, value=match)
        
        # 3. Ensure all messages are sent
        producer.flush()
        print("Data successfully sent to Kafka.")

    except Exception as e:
        print(f"Error occurred: {e}")

    # 4. Respect API rate limit (max 10 req/min)
    time.sleep(7)
