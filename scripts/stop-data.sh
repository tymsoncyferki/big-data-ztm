#!/usr/bin/env bash
# Stops only source simulator, leaving NiFi, Kafka, HDFS and GTFS schedule intact.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile ingestion stop simulator
