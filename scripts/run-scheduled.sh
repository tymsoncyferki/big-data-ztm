#!/usr/bin/env bash
# Run one complete batch inside Docker; lock and Spark/Hive execution are internal.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics run --rm --no-deps \
  batch-runner
