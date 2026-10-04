#!/bin/sh
# Creates all platform topics explicitly (auto-create is disabled).
# Safe to re-run: --if-not-exists.

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"   # use 3 in production (with min.insync.replicas=2)
KT=/opt/kafka/bin/kafka-topics.sh

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

for t in $TOPICS; do
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
done

echo "Topics ready:"
$KT --bootstrap-server "$BOOTSTRAP" --list
