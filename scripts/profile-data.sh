#!/usr/bin/env bash
# Read-only check of the cleaned data. Run this BEFORE build-ml-dataset.sh:
# it verifies that GPS trip_id values exist in the GTFS timetable.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner /opt/spark/bin/spark-submit --master 'local[2]' \
  --driver-memory "${ML_DRIVER_MEMORY:-2g}" \
  /opt/project/spark/ml/profile_data.py "$@"
