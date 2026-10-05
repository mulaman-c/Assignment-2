#!/bin/bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"
TOPICS="
orders.created
orders.confirmed
orders.rejected
orders.preparing
orders.ready
orders.cancelled
payments.completed
payments.failed
delivery.assigned
delivery.picked_up
delivery.completed
orders.dlq
payments.dlq
delivery.dlq
notifications.dlq
"

for topic in $TOPICS; do
    kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
done

kafka-topics --bootstrap-server "$BOOTSTRAP" --list
