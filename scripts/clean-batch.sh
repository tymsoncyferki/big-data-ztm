#!/usr/bin/env bash
# Manually clean one HDFS JSON source (normal batch processes JSON + GTFS).
set -euo pipefail
cd "$(dirname "$0")/.."
[[ $# -le 1 ]] || { echo 'Usage: bash scripts/clean-batch.sh [all|gps|weather|air_quality]' >&2; exit 2; }
dataset="${1:-all}"
case "$dataset" in all|gps|weather|air_quality) ;; *) echo 'Invalid dataset' >&2; exit 2 ;; esac
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner python3 -u /opt/project/scripts/container_scheduler.py --dataset "$dataset"
