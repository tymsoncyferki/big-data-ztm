#!/usr/bin/env bash
# Show a few rows of /data/enriched/{stop_visits,ml_dataset,schedule_stops} in readable form.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/project/spark/ml/show_sample.py "$@" 2>&1 | grep -v 'INFO\|WARN'