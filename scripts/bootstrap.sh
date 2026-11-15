#!/usr/bin/env bash
# Start all infrastructure, run one batch, and independently toggle data/schedule.
# Host requirements: Bash + Docker Compose. No host Python, cron or flock.
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  cat <<'USAGE'
Usage: bash scripts/bootstrap.sh [--with-data | --no-data] [--with-schedule | --no-schedule]
  --with-data       Enable simulator and daily GTFS; download GTFS immediately if HDFS is empty.
  --no-data         (default) Stop simulator and the NiFi GTFS download schedule.
  --with-schedule   Start the Docker batch scheduler (every 15 minutes).
  --no-schedule     (default) Stop the optional Docker batch scheduler.

Always starts HDFS, Kafka and NiFi, initializes NiFi flows,
and runs one incremental Spark batch (JSON then GTFS). Optional services can be toggled later using
scripts/start-data.sh, stop-data.sh, start-schedule.sh, stop-schedule.sh.
USAGE
}

data_mode=no-data
schedule_mode=no-schedule
seen_data=0
seen_schedule=0
for flag in "$@"; do
  case "$flag" in
    --with-data|--no-data)
      if (( seen_data )); then echo 'Specify at most one data flag.' >&2; exit 2; fi
      data_mode="${flag#--}"; seen_data=1 ;;
    --with-schedule|--no-schedule)
      if (( seen_schedule )); then echo 'Specify at most one schedule flag.' >&2; exit 2; fi
      schedule_mode="${flag#--}"; seen_schedule=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

[[ -f .env ]] || { echo 'Missing .env: copy .env.example and set secrets.' >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source .env
set +a
require_secret() {
  local value="${!1:-}"
  if [[ -z "$value" || "$value" == REPLACE_* ]]; then
    echo "Set $1 to a non-placeholder value in .env." >&2; exit 1
  fi
}
for name in NIFI_USERNAME NIFI_PASSWORD NIFI_SENSITIVE_PROPS_KEY; do
  require_secret "$name"
done
if [[ ${#NIFI_PASSWORD} -lt 12 || ${#NIFI_SENSITIVE_PROPS_KEY} -lt 12 ]]; then
  echo 'NIFI_PASSWORD and NIFI_SENSITIVE_PROPS_KEY must be at least 12 characters.' >&2; exit 1
fi
if [[ "$data_mode" == with-data ]]; then require_secret WEATHER_API_TOKEN; fi

compose=(docker compose -f docker-compose.yml)
# An old checkout may have installed a host-cron entry. Remove only our marker
# when crontab happens to exist; cron is not required in this version.
if command -v crontab >/dev/null 2>&1; then
  existing="$(crontab -l 2>/dev/null || true)"
  if grep -Fq '# BIG_DATA_ZTM_BATCH_SCHEDULER' <<<"$existing"; then
    echo 'Removing legacy host-cron schedule to prevent duplicate executions...'
    remaining="$(printf '%s\n' "$existing" | grep -vF '# BIG_DATA_ZTM_BATCH_SCHEDULER' || true)"
    if ! printf '%s\n' "$remaining" | crontab -; then
      echo 'WARNING: legacy cron entry could not be removed; disable it manually.' >&2
    fi
  fi
fi

echo '[1/7] Starting HDFS, Kafka and NiFi...'
"${compose[@]}" up -d --build --remove-orphans

echo '[2/7] Preparing Kafka topics...'
bash scripts/init-kafka.sh

echo '[3/7] Preparing HDFS and Spark batch image...'
bash scripts/setup-batch.sh

echo '[4/7] Importing NiFi flows using containerized Python...'
"${compose[@]}" --profile tools build nifi-init
"${compose[@]}" --profile tools run --rm --no-deps nifi-init

if [[ "$data_mode" == with-data ]]; then
  echo '[5/7] Starting optional data acquisition (simulator + GTFS)...'
  bash scripts/start-data.sh
else
  echo '[5/7] Stopping optional data acquisition...'
  bash scripts/stop-data.sh
fi

echo '[6/7] Running one incremental Spark batch...'
bash scripts/run-scheduled.sh

if [[ "$schedule_mode" == with-schedule ]]; then
  echo '[7/7] Starting Docker batch scheduler...'
  bash scripts/start-schedule.sh
else
  echo '[7/7] Stopping optional Docker batch scheduler...'
  bash scripts/stop-schedule.sh
fi

echo 'Bootstrap completed.'
echo 'NiFi: https://localhost:8443/nifi | HDFS: http://localhost:9870'
echo "Simulator: $data_mode | Docker schedule: $schedule_mode"
echo 'One-off batch: bash scripts/run-scheduled.sh'
