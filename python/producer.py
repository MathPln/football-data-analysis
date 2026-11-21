import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Configuration
load_dotenv()
API_KEY = os.getenv("API_KEY")
BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
MATCHES_TOPIC = os.getenv("KAFKA_TOPIC", "laliga-matches")
SCORERS_TOPIC = os.getenv("KAFKA_SCORERS_TOPIC", "laliga-scorers")

if not API_KEY or API_KEY == "xxx":
    raise ValueError("API_KEY is not set. Please set it in the .env file.")
if not BROKER or BROKER == "xxx":
    raise ValueError("KAFKA_BROKER is not set. Please set it in the .env file.")

MATCHES_URL = "https://api.football-data.org/v4/competitions/PD/matches"
SCORERS_URL = "https://api.football-data.org/v4/competitions/PD/scorers?limit=50"
HEADERS = {'X-Auth-Token': API_KEY}

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

print(f"Starting Football API Producer on {BROKER}...")

try:
    while True:
        try:
            # 1. Fetch & send matches
            res_matches = requests.get(MATCHES_URL, headers=HEADERS, timeout=15)
            res_matches.raise_for_status()
            matches = res_matches.json().get('matches', [])

            for match in matches:
                match_id = match.get('id', 'unknown')
                producer.send(MATCHES_TOPIC, key=match_id, value=match)

            # 2. Fetch & send top scorers (for Player Stats page)
            res_scorers = requests.get(SCORERS_URL, headers=HEADERS, timeout=15)
            res_scorers.raise_for_status()
            scorers = res_scorers.json().get('scorers', [])

            for scorer in scorers:
                player_id = scorer.get('player', {}).get('id', 'unknown')
                producer.send(SCORERS_TOPIC, key=player_id, value=scorer)

            # 3. Flush all messages
            producer.flush()
            print(f"Sent {len(matches)} matches to '{MATCHES_TOPIC}' and {len(scorers)} players to '{SCORERS_TOPIC}'.")

        except Exception as e:
            print(f"Error occurred: {e}")

        # 4. Wait 60s between polls to stay well under API rate limits
        time.sleep(60)

except KeyboardInterrupt:
    print("\nStopping Producer.")
    producer.close()