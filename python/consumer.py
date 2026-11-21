import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load variables from the .env file
load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
MATCHES_TOPIC = os.getenv("KAFKA_TOPIC", "laliga-matches")
SCORERS_TOPIC = os.getenv("KAFKA_SCORERS_TOPIC", "laliga-scorers")
GROUP_ID = "football-analysts"

consumer = KafkaConsumer(
    MATCHES_TOPIC,
    SCORERS_TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='earliest',
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Consumer started (Group: {GROUP_ID}) on broker {BROKER}")
print(f"Listening to topics: '{MATCHES_TOPIC}', '{SCORERS_TOPIC}'...\n")

matches_processed = 0
players_processed = 0

try:
    for message in consumer:
        data = message.value

        if message.topic == MATCHES_TOPIC:
            matches_processed += 1
            home = data.get('homeTeam', {}).get('shortName', 'Home')
            away = data.get('awayTeam', {}).get('shortName', 'Away')
            md = data.get('matchday', '?')
            score = data.get('score', {}).get('fullTime', {})
            hg = score.get('home')
            ag = score.get('away')
            score_str = f"{hg}-{ag}" if hg is not None and ag is not None else "vs"
            print(f"[Match {matches_processed}] MD {md} | {home} {score_str} {away} ({data.get('status', '?')})")

        elif message.topic == SCORERS_TOPIC:
            players_processed += 1
            player = data.get('player', {}).get('name', 'Unknown')
            club = data.get('team', {}).get('shortName', '?')
            goals = data.get('goals', 0)
            assists = data.get('assists', 0) or 0
            print(f"[Player {players_processed}] {player} ({club}) | {goals}G / {assists}A")

except KeyboardInterrupt:
    print(f"\nStopping Consumer. Total processed: {matches_processed} matches, {players_processed} players.")
    consumer.close()