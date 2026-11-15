#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.yml --profile analytics)
echo 'Waiting for HiveServer2 to accept SQL connections...'
for attempt in $(seq 1 80); do
  if "${compose[@]}" exec -T hive-server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default' \
      -e 'SHOW DATABASES;' >/dev/null 2>&1; then
    break
  fi
  if (( attempt == 80 )); then
    echo 'HiveServer2 did not become ready; check docker compose --profile analytics logs hive-server2 hive-metastore hive-db' >&2
    exit 1
  fi
  sleep 5
done
echo 'Creating external cleaned tables...'
"${compose[@]}" exec -T hive-server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default' \
  -f /opt/hive/project-sql/cleaned_tables.sql
