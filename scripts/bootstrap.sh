#!/usr/bin/env bash
# Start the complete infrastructure and import NiFi flows.
# Data acquisition is opt-in: --with-data.
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  cat <<'USAGE'
Usage: bash scripts/bootstrap.sh [--no-data | --with-data]

  (no flag) / --no-data   Start NiFi, HDFS and Kafka; import configuration.
                          Do NOT start the Python simulator.
  --with-data             Do all of the above, then start Python simulator
                          (GPS/weather/air-quality API acquisition).

The GTFS daily 03:00 schedule configured in NiFi is unchanged in BOTH modes.
USAGE
}

MODE="no-data"
if [[ $# -gt 1 ]]; then usage >&2; exit 2; fi
case "${1:---no-data}" in
  --no-data) MODE="no-data" ;;
  --with-data) MODE="with-data" ;;
  -h|--help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

if [[ ! -f .env ]]; then
  echo 'Missing .env: run `cp .env.example .env` and set your credentials.' >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source .env
set +a
for var in NIFI_USERNAME NIFI_PASSWORD NIFI_SENSITIVE_PROPS_KEY; do
  value="${!var:-}"
  if [[ -z "$value" || "$value" == REPLACE_* ]]; then
    echo "Set $var to a real value in .env." >&2
    exit 1
  fi
done
if [[ ${#NIFI_PASSWORD} -lt 12 || ${#NIFI_SENSITIVE_PROPS_KEY} -lt 12 ]]; then
  echo 'NIFI_PASSWORD and NIFI_SENSITIVE_PROPS_KEY must have at least 12 characters.' >&2
  exit 1
fi
if [[ "$MODE" == "with-data" ]]; then
  if [[ -z "${WEATHER_API_TOKEN:-}" || "${WEATHER_API_TOKEN}" == REPLACE_* ]]; then
    echo 'Set WEATHER_API_TOKEN in .env before using --with-data.' >&2
    exit 1
  fi
fi

compose=(docker compose -f docker-compose.yml)

echo 'Starting NiFi, HDFS and Kafka (simulator is opt-in)...'
"${compose[@]}" up -d --build

echo 'Preparing Kafka topics...'
bash scripts/init-kafka.sh

echo 'Preparing HDFS directories...'
bash scripts/init-hdfs.sh

echo 'Importing/starting NiFi flows (idempotent on existing portable installation)...'
NIFI_API_URL=https://localhost:8443/nifi-api \
  python3 scripts/init-nifi.py

if [[ "$MODE" == "with-data" ]]; then
  echo 'Starting the Python simulator (GPS 10 s, weather/air 30 s, 15-min buffer)...'
  "${compose[@]}" --profile ingestion up -d --build --no-deps simulator
  echo 'Simulator started. Logs: docker compose logs -f simulator'
else
  # A second bootstrap with --no-data must not leave a previously started
  # simulator ingesting data silently. This does not touch NiFi/GTFS.
  running_simulator="$("${compose[@]}" --profile ingestion ps -q simulator 2>/dev/null || true)"
  if [[ -n "$running_simulator" ]]; then
    echo 'Stopping previously running simulator (no-data mode; in-RAM buffer will be lost)...'
    "${compose[@]}" --profile ingestion stop simulator
  fi
  echo 'Infrastructure ready, Python simulator NOT started.'
  echo "To enable acquisition: bash scripts/start-data.sh"
fi

echo 'NiFi UI: https://localhost:8443/nifi | NameNode UI: http://localhost:9870'
