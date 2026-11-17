#!/usr/bin/env bash
# Build the ML dataset (stop visits + delays + features + target) from the cleaned data.
# Safe to re-run: the chosen date partitions are replaced, nothing else is touched.
# Waits for the 15-minute cleaning job to finish (shared lock) instead of colliding with it.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner /opt/spark/bin/spark-submit --master 'local[2]' \
  --driver-memory "${ML_DRIVER_MEMORY:-2g}" \
  /opt/project/spark/ml/build_ml_dataset.py "$@"
# Examples:
#   bash scripts/build-ml-dataset.sh                                  # all available days
#   bash scripts/build-ml-dataset.sh --start-date 2026-11-16 --end-date 2026-11-17
#   bash scripts/build-ml-dataset.sh --horizon-min 15 --stop-radius-m 60
