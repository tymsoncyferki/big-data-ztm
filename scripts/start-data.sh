#!/usr/bin/env bash
# Starts the simulator only; requires infrastructure to have been bootstrapped.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f .env ]]; then echo 'Missing .env' >&2; exit 1; fi
set -a
# shellcheck disable=SC1091
source .env
set +a
if [[ -z "${WEATHER_API_TOKEN:-}" || "${WEATHER_API_TOKEN}" == REPLACE_* ]]; then
  echo 'Set WEATHER_API_TOKEN in .env before acquiring data.' >&2; exit 1
fi
docker compose -f docker-compose.yml --profile ingestion up -d --build --no-deps simulator
