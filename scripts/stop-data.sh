#!/usr/bin/env bash
# Stop the simulator and NiFi GTFS daily download; do not stop Kafka/HDFS.
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.yml)
"${compose[@]}" --profile ingestion stop simulator
"${compose[@]}" --profile tools run --rm --no-deps nifi-init \
  python -u scripts/init-nifi.py --action stop-gtfs
