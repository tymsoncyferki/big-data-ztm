#!/usr/bin/env bash
# Disable only the Docker scheduler. Existing batch tasks finish or stop on SIGTERM.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml --profile analytics --profile scheduler stop batch-scheduler heartbeat
echo 'Docker batch scheduler stopped.'
