#!/usr/bin/env bash
# Prepare persistent HDFS state folders shared by Spark and the Docker batch scheduler.
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.yml)
root=/data/cleaned/_pipeline_state
"${compose[@]}" exec -T namenode hdfs dfs -mkdir -p \
  "$root/raw" "$root/gtfs" "$root/staging" "$root/backups"
# Development cluster: spark and the namenode HDFS client use supergroup.
"${compose[@]}" exec -T namenode hdfs dfs -chmod 775 \
  "$root" "$root/raw" "$root/gtfs" "$root/staging" "$root/backups"
