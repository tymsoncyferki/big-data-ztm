#!/usr/bin/env bash
# Enable the optional schedule without restarting HDFS or NiFi.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics --profile scheduler up -d --no-deps batch-scheduler heartbeat
echo 'Docker batch scheduler is running (quarter-hour boundaries).'
