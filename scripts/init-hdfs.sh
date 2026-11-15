#!/usr/bin/env bash
# Create HDFS parent directories; GTFS tables appear only when source files exist.
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.yml)
for attempt in $(seq 1 60); do
  if "${compose[@]}" exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; then
    break
  fi
  if (( attempt == 60 )); then
    echo 'HDFS not ready; inspect NameNode/DataNode logs.' >&2
    exit 1
  fi
  sleep 2
done
paths=(/data/raw /data/cleaned /data/enriched /data/enriched/gtfs_trip_services)
for name in gps weather air_quality gtfs; do paths+=("/data/raw/$name"); done
for name in gps_observations weather_observations air_quality_observations; do
  paths+=("/data/cleaned/$name")
done
"${compose[@]}" exec -T namenode hdfs dfs -mkdir -p "${paths[@]}"
"${compose[@]}" exec -T namenode hdfs dfs -chmod 775 \
  /data/cleaned /data/enriched /data/enriched/gtfs_trip_services \
  /data/cleaned/gps_observations /data/cleaned/weather_observations \
  /data/cleaned/air_quality_observations
echo 'HDFS Raw, Cleaned and Enriched parent directories ready.'
