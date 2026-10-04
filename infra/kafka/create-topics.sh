#!/bin/sh
# Creates saga topics and their DLQs. Idempotent (--if-not-exists), safe to re-run.
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
TOPICS="order.events inventory.events payment.events shipping.events"

create() {
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

for t in $TOPICS; do
  create "$t" "$PARTITIONS"
  create "$t.dlq" 1
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
