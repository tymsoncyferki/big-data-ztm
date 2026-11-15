#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics exec -T hive-server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default' \
  -f /opt/hive/project-sql/refresh_partitions.sql
