#!/bin/bash
# wait for kafka
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

TOPICS=("activation.requested" "billing.events" "provisioning.events" "activation.events" "activation.requested.dlq" "billing.events.dlq" "provisioning.events.dlq" "activation.events.dlq")

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

echo "Topics created successfully."
