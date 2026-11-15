#!/usr/bin/env bash
set -euo pipefail
compose=(docker compose -f docker-compose.yml)
for attempt in $(seq 1 45); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" -eq 45 ]]; then echo "Kafka not ready" >&2; exit 1; fi
  sleep 2
done
for topic in gps weather air_quality; do
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done
