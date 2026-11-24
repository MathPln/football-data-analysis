
import os
import time
import json
import socket
from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TEAM_MATCHES_TOPIC = os.getenv("KAFKA_TEAM_MATCHES_TOPIC", "laliga-team-matches")
SCORERS_TOPIC = os.getenv("KAFKA_SCORERS_TOPIC", "laliga-scorers")
TEAM_STATS_TOPIC = os.getenv("KAFKA_TEAM_STATS_TOPIC", "laliga-team-stats")
GROUP_ID = "football-analysts"
INSTANCE_ID = socket.gethostname()

# Each match is stored once per team. This state is rebuilt from Kafka on
# startup and after rebalances, rather than relying on committed offsets.
team_matches = {}
events_processed = 0
players_processed = 0


class ReplayOnAssignment(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print(f"[{INSTANCE_ID}] Partitions revoked: {sorted(map(str, revoked))}")

    def on_partitions_assigned(self, assigned):
        # A team ID always maps to the same partition. Rebuild the state of
        # all assigned match partitions from their earliest records.
        # This also covers consumer restarts with already committed offsets.
        team_matches.clear()
        match_partitions = [
            tp for tp in assigned if tp.topic == TEAM_MATCHES_TOPIC
        ]
        if match_partitions:
            consumer.seek_to_beginning(*match_partitions)

        print(
            f"[{INSTANCE_ID}] Assigned partitions: "
            f"{sorted(map(str, assigned))}; replaying match history"
        )


def compute_team_stats(team_id, matches_dict):
    ordered = sorted(
        matches_dict.values(),
        key=lambda m: m.get('utcDate') or ''
    )
    first = ordered[0]

    stats = {
        'teamId': team_id,
        'name': first['teamName'],
        'tla': first.get('tla') or first['teamName'][:3].upper(),
        'played': 0, 'won': 0, 'draw': 0, 'lost': 0,
        'gf': 0, 'ga': 0, 'gd': 0, 'points': 0,
        'homeW': 0, 'homeD': 0, 'homeL': 0,
        'awayW': 0, 'awayD': 0, 'awayL': 0,
        'cleanSheets': 0, 'last5': '', 'history': [],
        'processedBy': INSTANCE_ID
    }

    form = []

    for m in ordered:
        gf, ga = m.get('goalsFor'), m.get('goalsAgainst')

        if m.get('status') != 'FINISHED' or gf is None or ga is None:
            continue

        is_home = m.get('venue') == 'Home'

        stats['played'] += 1
        stats['gf'] += gf
        stats['ga'] += ga

        if ga == 0:
            stats['cleanSheets'] += 1

        if gf > ga:
            res = 'W'
            stats['won'] += 1
            stats['points'] += 3
            stats['homeW' if is_home else 'awayW'] += 1

        elif gf < ga:
            res = 'L'
            stats['lost'] += 1
            stats['awayL' if not is_home else 'homeL'] += 1

        else:
            res = 'D'
            stats['draw'] += 1
            stats['points'] += 1
            stats['homeD' if is_home else 'awayD'] += 1

        form.append(res)

        stats['history'].append({
            'MD': m.get('matchday', 0),
            'Date': (m.get('utcDate') or '')[:10] or '-',
            'Venue': m.get('venue', '-'),
            'Opponent': m.get('opponent', '-'),
            'Score': f"{gf} - {ga}",
            'Result': res
        })

    stats['gd'] = stats['gf'] - stats['ga']
    stats['last5'] = ' '.join(form[-5:]) or '-'

    return stats


consumer = None
producer = None

for attempt in range(1, 16):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            acks='all',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )

        consumer.subscribe(
            topics=[TEAM_MATCHES_TOPIC, SCORERS_TOPIC],
            listener=ReplayOnAssignment()
        )

        break

    except Exception as e:
        if consumer is not None:
            consumer.close()
            consumer = None

        if producer is not None:
            producer.close()
            producer = None

        print(
            f"[{INSTANCE_ID}] Kafka not ready ({attempt}/15): "
            f"{e}. Retrying in 3s..."
        )
        time.sleep(3)

if consumer is None or producer is None:
    raise RuntimeError(f"Could not connect to Kafka broker at {BROKER}")

print(f"[{INSTANCE_ID}] Consumer started (Group: {GROUP_ID}) on broker {BROKER}")
print(
    f"[{INSTANCE_ID}] Listening to: '{TEAM_MATCHES_TOPIC}', "
    f"'{SCORERS_TOPIC}' -> Publishing to: '{TEAM_STATS_TOPIC}'\n"
)

try:
    for message in consumer:
        data = message.value
        meta = f"P{message.partition}:O{message.offset}"

        if message.topic == TEAM_MATCHES_TOPIC and 'teamId' in data:
            events_processed += 1

            tid = data['teamId']
            team_matches.setdefault(tid, {})[data['matchId']] = data

            if data.get('status') == 'FINISHED':
                stats = compute_team_stats(tid, team_matches[tid])

                # Wait for Kafka to acknowledge the output before
                # committing the input offset.
                producer.send(
                    TEAM_STATS_TOPIC,
                    key=tid,
                    value=stats
                ).get(timeout=15)

                print(
                    f"[{INSTANCE_ID}][{meta}] {stats['name']} updated: "
                    f"{stats['points']} pts "
                    f"({stats['played']}P, GD {stats['gd']})"
                )

        elif message.topic == SCORERS_TOPIC:
            players_processed += 1

            player = data.get('player', {}).get('name', 'Unknown')
            club = data.get('team', {}).get('shortName', '?')
            goals = data.get('goals', 0)
            assists = data.get('assists', 0) or 0

            print(
                f"[{INSTANCE_ID}][{meta}][Player {players_processed}] "
                f"{player} ({club}) | {goals}G / {assists}A"
            )

        consumer.commit()

except KeyboardInterrupt:
    print(
        f"\nStopping Consumer. Total processed: "
        f"{events_processed} team-match events, "
        f"{players_processed} players."
    )

finally:
    consumer.close()
    producer.close()


