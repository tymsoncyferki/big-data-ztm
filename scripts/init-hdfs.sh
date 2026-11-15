#!/usr/bin/env bash
# Wait for HDFS and create raw and external-table paths (safe to rerun).
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.yml)
for attempt in $(seq 1 60); do
  if "${compose[@]}" exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'OFF'; then
    break
  fi
  if (( attempt == 60 )); then
    echo 'HDFS not ready; check NameNode and DataNode logs.' >&2
    exit 1
  fi
  sleep 2
done

raw=(gps weather air_quality gtfs)
cleaned=(gps_observations weather_observations air_quality_observations
         gtfs_routes gtfs_stops gtfs_trips gtfs_stop_times gtfs_calendar gtfs_calendar_dates)
paths=(/data/warehouse /data/raw /data/cleaned)
clean_paths=()
for dataset in "${raw[@]}"; do paths+=("/data/raw/$dataset"); done
for dataset in "${cleaned[@]}"; do
  clean_paths+=("/data/cleaned/$dataset")
done
paths+=("${clean_paths[@]}")
"${compose[@]}" exec -T namenode hdfs dfs -mkdir -p "${paths[@]}"
# Development cluster: Spark, Hive and HDFS clients share the 'supergroup' group.
"${compose[@]}" exec -T namenode hdfs dfs -chmod 775 /data/warehouse /data/cleaned \
  "${clean_paths[@]}"
echo 'Raw and cleaned HDFS directories are ready.'
