#!/bin/bash
# Crea los 4 topics de negocio y sus DLQ: 3 particiones y retención de 7 días.
set -e
BOOTSTRAP=kafka:9092
RETENTION_MS=604800000   # 7 días (coincide con el TTL de processed_events)

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list

TOPICS=("activation.requested" "billing.events" "provisioning.events" "activation.events")

for topic in "${TOPICS[@]}" ; do
  for t in "$topic" "$topic.dlq"; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$t" --partitions 3 --replication-factor 1 --config retention.ms=$RETENTION_MS
    # Si el topic ya existía (de una corrida anterior), igual le aplicamos la retención
    /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics \
      --entity-name "$t" --add-config retention.ms=$RETENTION_MS
  done
done

echo "Topics created successfully."
