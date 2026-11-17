#!/usr/bin/env bash
# Read-only diagnosis of delay quality (run after build-ml-dataset.sh).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner /opt/spark/bin/spark-submit --master 'local[2]' \
  --driver-memory "${ML_DRIVER_MEMORY:-2g}" \
  /opt/project/spark/ml/diagnose_delays.py "$@"
