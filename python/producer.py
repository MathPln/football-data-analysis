import os
import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
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

# Initialize Kafka Producer (with retry for Docker startup)
producer = None
for attempt in range(1, 16):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            acks='all',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        break
    except NoBrokersAvailable:
        print(f"Kafka not ready ({attempt}/15), retrying in 3s...")
        time.sleep(3)

if producer is None:
    raise RuntimeError(f"Could not connect to Kafka broker at {BROKER}")

print(f"Starting Football API Producer on {BROKER}...")

last_matches = {}
last_scorers = {}

try:
    while True:
        try:
            # 1. Fetch & send matches
            res_matches = requests.get(MATCHES_URL, headers=HEADERS, timeout=15)
            res_matches.raise_for_status()
            matches = res_matches.json().get('matches', [])

            sent_matches = 0
            for match in matches:
                match_id = match.get('id', 'unknown')
                raw = json.dumps(match, sort_keys=True)
                if last_matches.get(match_id) != raw:
                    producer.send(MATCHES_TOPIC, key=match_id, value=match)
                    last_matches[match_id] = raw
                    sent_matches += 1

            # 2. Fetch & send top scorers (for Player Stats page)
            res_scorers = requests.get(SCORERS_URL, headers=HEADERS, timeout=15)
            res_scorers.raise_for_status()
            scorers = res_scorers.json().get('scorers', [])

            sent_scorers = 0
            for scorer in scorers:
                player_id = scorer.get('player', {}).get('id', 'unknown')
                raw = json.dumps(scorer, sort_keys=True)
                if last_scorers.get(player_id) != raw:
                    producer.send(SCORERS_TOPIC, key=player_id, value=scorer)
                    last_scorers[player_id] = raw
                    sent_scorers += 1

            # 3. Flush all messages
            if sent_matches or sent_scorers:
                producer.flush()
            print(f"Sent {sent_matches}/{len(matches)} matches to '{MATCHES_TOPIC}' and {sent_scorers}/{len(scorers)} players to '{SCORERS_TOPIC}'.")

        except Exception as e:
            print(f"Error occurred: {e}")

        # 4. Wait 60s between polls to stay well under API rate limits
        time.sleep(60)

except KeyboardInterrupt:
    print("\nStopping Producer.")
    producer.close()