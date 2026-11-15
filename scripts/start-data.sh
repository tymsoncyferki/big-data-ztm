#!/usr/bin/env bash
# Enable optional data acquisition: simulator + NiFi daily GTFS download.
# If HDFS contains no GTFS ZIP, request one immediate NiFi run and wait for it.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo 'Missing .env' >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source .env
set +a
if [[ -z "${WEATHER_API_TOKEN:-}" || "${WEATHER_API_TOKEN}" == REPLACE_* ]]; then
  echo 'Set WEATHER_API_TOKEN in .env before enabling source acquisition.' >&2
  exit 1
fi

compose=(docker compose -f docker-compose.yml)

# NiFi writes the archive using writeAndRename; only final nonempty ZIP files count.
archive_exists() {
  "${compose[@]}" exec -T namenode hdfs dfs -ls -R /data/raw/gtfs 2>/dev/null \
    | awk '$1 ~ /^-/ && $5 + 0 > 0 && $NF ~ /\.zip$/ {found=1} END {exit !found}'
}

control=("${compose[@]}" --profile tools run --rm --no-deps nifi-init \
         python -u scripts/init-nifi.py --action start-gtfs)

if archive_exists; then
  echo '[GTFS] Existing nonempty ZIP found in HDFS; skipping initial download.'
else
  echo '[GTFS] No ZIP found in HDFS; requesting initial NiFi download.'
  control+=(--initial-download)
  needs_archive=1
fi

# GTFS ingestion and the simulator can later be stopped independently of the
# batch schedule. NiFi's CRON trigger is only enabled when data mode is on.
"${control[@]}"
"${compose[@]}" --profile ingestion up -d --build --no-deps simulator

if [[ "${needs_archive:-0}" == 1 ]]; then
  echo '[GTFS] Waiting for the archived ZIP in HDFS (up to 300 seconds)...'
  for (( attempt=1; attempt<=60; attempt++ )); do
    if archive_exists; then
      echo '[GTFS] Initial ZIP is available in HDFS.'
      "${compose[@]}" --profile tools run --rm --no-deps nifi-init \
        python -u scripts/init-nifi.py --action enable-gtfs-trigger
      exit 0
    fi
    sleep 5
  done
  echo '[GTFS] No ZIP appeared within 300 seconds. Check NiFi Download/PutHDFS bulletins and the source URL.' >&2
  exit 1
fi
