import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load variables from the .env file
load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "laliga-matches")
GROUP_ID = "football-analysts"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='earliest',
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Consumer started (Group : {GROUP_ID}) on broker {BROKER}")
print(f"Waiting for matches on topic '{TOPIC}'...\n")

matches_processed = 0

try:
    for message in consumer:
        match = message.value
        matches_processed += 1
        
        home_team = match.get('homeTeam', {}).get('shortName', 'Domicile')
        away_team = match.get('awayTeam', {}).get('shortName', 'Extérieur')
        matchday = match.get('matchday', '?')
        
        print(f"[Match {matches_processed}] Matchday {matchday} | {home_team} vs {away_team}")
        
except KeyboardInterrupt:
    print(f"\nStopping Consumer. Total processed: {matches_processed} matches.")