#!/usr/bin/env bash
# Heartbeat script pings healthchecks.io every 15 minutes
# HC_DOCKER_URL - always pinged, shows that docker and the stack are up
# HC_DATA_URL - pinged only when a new GPS snapshot landed in HDFS since the last check (confirms that NIFI is working)
# Alerts are configured on the healthchecks.io site
set -u

GPS_DIR=hdfs://namenode:8020/data/raw/gps
INTERVAL=900

# print the last (newest by name) subdirectory of HDFS dir
last_dir() {
  hdfs dfs -ls "$1" 2>/dev/null | awk '$1 ~ /^d/ {print $8}' | sort | tail -1
}

ping() {
  local url=$1 body=$2
  if [[ -n "$url" ]]; then
    curl -fsS -m 10 --retry 3 -o /dev/null --data-raw "$body" "$url" || echo "ping failed: $url"
  fi
}

while true; do
  ping "${HC_DOCKER_URL:-}" "docker up"

  # newest GPS file is in the newest date=/hour= partition
  date_dir=$(last_dir "$GPS_DIR")
  hour_dir=$(last_dir "$date_dir")
  newest=$(hdfs dfs -ls "$hour_dir" 2>/dev/null | awk '$1 ~ /^-/ {print $6 " " $7}' | sort | tail -1)
  since=$(date -d "-$INTERVAL sec" '+%Y-%m-%d %H:%M')

  if [[ -n "$newest" && "$newest" > "$since" ]]; then
    ping "${HC_DATA_URL:-}" "newest GPS file: $newest"
  else
    echo "$(date '+%F %T') no new GPS data since $since (newest: ${newest:-none})"
  fi

  sleep "$INTERVAL"
done
