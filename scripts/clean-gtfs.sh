#!/usr/bin/env bash
# Clean one GTFS archive, or all unprocessed archives if none is specified.
set -euo pipefail
cd "$(dirname "$0")/.."
args=(--dataset gtfs)
if [[ "${1:-}" == --force ]]; then args+=(--force); shift; fi
[[ $# -le 1 ]] || { echo 'Usage: bash scripts/clean-gtfs.sh [--force] [hdfs://.../archive.zip]' >&2; exit 2; }
if [[ $# -eq 1 ]]; then args+=(--archive "$1"); fi
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner python3 -u /opt/project/scripts/container_scheduler.py "${args[@]}"
