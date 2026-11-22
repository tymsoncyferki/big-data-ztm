#!/usr/bin/env bash
# Read-only statistics of the source datasets for the Milestone 2 report.
# The JSON goes to docs/milestone_2/dataset_stats.json (override with OUT=...),
# Spark logs stay on the terminal. Takes several minutes (raw JSON of one day is read).
#   bash scripts/describe-sources.sh [--raw-day YYYY-MM-DD] [--detail-days 2] [--skip-raw]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="${OUT:-docs/milestone_2/dataset_stats.json}"
mkdir -p "$(dirname "$OUT")"
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps -T \
  batch-runner /opt/spark/bin/spark-submit --master 'local[2]' \
  --driver-memory "${ML_DRIVER_MEMORY:-2g}" \
  /opt/project/spark/ml/describe_sources.py "$@" > "$OUT.tmp"
mv "$OUT.tmp" "$OUT"
echo "Saved $OUT ($(wc -c < "$OUT") bytes)"
