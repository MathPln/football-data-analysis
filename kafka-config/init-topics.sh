#!/bin/sh
set -e

BOOTSTRAP="kafka:9092"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
KAFKA_CONFIGS="/opt/kafka/bin/kafka-configs.sh"

# Create topics if they do not exist
create_topic() {
    if ! "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
        --describe --topic "$1" >/dev/null 2>&1; then
        "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
            --create --topic "$1" \
            --partitions 3 --replication-factor 1
    fi
}

create_topic laliga-team-matches
create_topic laliga-team-stats
create_topic laliga-scorers

# Keep match history for 365 days
"$KAFKA_CONFIGS" --bootstrap-server "$BOOTSTRAP" \
    --entity-type topics --entity-name laliga-team-matches \
    --alter --add-config cleanup.policy=delete,retention.ms=31536000000

# Keep latest team statistics per team
"$KAFKA_CONFIGS" --bootstrap-server "$BOOTSTRAP" \
    --entity-type topics --entity-name laliga-team-stats \
    --alter --add-config cleanup.policy=compact

# Keep latest scorer statistics per player
"$KAFKA_CONFIGS" --bootstrap-server "$BOOTSTRAP" \
    --entity-type topics --entity-name laliga-scorers \
    --alter --add-config cleanup.policy=compact

echo "Kafka topics configured successfully."
