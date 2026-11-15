#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics --profile scheduler stop batch-scheduler hive-server2 hive-metastore hive-db
echo 'Stopped analytics only. HDFS, Kafka, NiFi and their data are untouched.'
