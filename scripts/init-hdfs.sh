#!/usr/bin/env bash
set -euo pipefail
compose=(docker compose -f docker-compose.yml)
for attempt in $(seq 1 60); do
  if "${compose[@]}" exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'OFF'; then
    break
  fi
  if [[ "$attempt" -eq 60 ]]; then echo "HDFS not ready; check namenode/datanode logs" >&2; exit 1; fi
  sleep 2
done
"${compose[@]}" exec -T namenode hdfs dfs -mkdir -p \
  /data/raw/gps /data/raw/weather /data/raw/air_quality /data/raw/gtfs
"${compose[@]}" exec -T namenode hdfs dfs -ls /data/raw
