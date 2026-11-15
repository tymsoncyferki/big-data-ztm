#!/usr/bin/env bash
# Prepare HDFS directories and build the Spark-only batch container.
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/init-hdfs.sh
bash scripts/init-pipeline-state.sh
docker compose -f docker-compose.yml --profile analytics build batch-runner
echo 'HDFS data directories and Spark batch image ready.'
