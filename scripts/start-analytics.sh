#!/usr/bin/env bash
# Idempotent analytics bootstrap, also usable independently of bootstrap.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
if [[ -z "${HIVE_DB_PASSWORD:-}" || "${HIVE_DB_PASSWORD}" == REPLACE_* ]]; then
  echo 'Set HIVE_DB_PASSWORD in .env before starting Hive.' >&2
  exit 1
fi
compose=(docker compose -f docker-compose.yml --profile analytics)
echo 'Preparing HDFS directories...'
bash scripts/init-hdfs.sh
bash scripts/init-pipeline-state.sh
echo 'Starting PostgreSQL, Hive Metastore and HiveServer2...'
"${compose[@]}" up -d --build hive-db hive-metastore hive-server2
bash scripts/init-hive.sh
echo 'Registering existing HDFS partitions in Hive...'
bash scripts/refresh-hive.sh
echo 'Building shared Spark/Beeline batch image...'
"${compose[@]}" build batch-runner
echo 'Hive is ready. Batch runner and Docker scheduler image are available.'
echo 'Hive JDBC: jdbc:hive2://localhost:10000/default'
