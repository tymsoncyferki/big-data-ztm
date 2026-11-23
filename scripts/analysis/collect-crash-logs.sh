#!/usr/bin/env bash
# Collects logs from a given time window after a crash (run inside WSL).
# Run it BEFORE recreating containers, `docker compose up -d` with changes wipes docker logs.
# Usage: bash scripts/analysis/collect-crash-logs.sh "2026-11-16 21:30" "2026-11-16 23:00"

set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ $# -ne 2 ]]; then
  echo 'Usage: bash scripts/analysis/collect-crash-logs.sh "YYYY-MM-DD HH:MM" "YYYY-MM-DD HH:MM"'
  exit 1
fi

# local time -> RFC3339 with offset, so docker does not guess the timezone
since=$(date -d "$1" --iso-8601=seconds) || exit 1
until=$(date -d "$2" --iso-8601=seconds) || exit 1
out="$HOME/crash-logs/$(date -d "$1" +%Y-%m-%d_%H%M)"
mkdir -p "$out"
echo "Window: $since -> $until"
echo "Output: $out"

# container logs + state (OOMKilled, exit code, restarts)
for c in $(docker ps -a --filter label=com.docker.compose.project=big-data-ztm --format '{{.Names}}'); do
  docker logs --timestamps --since "$since" --until "$until" "$c" > "$out/$c.log" 2>&1
  docker inspect -f '{{.Name}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}} restarts={{.RestartCount}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}' "$c"
done > "$out/containers-state.txt"

# journal from the window, any boot (only if journald storage is persistent)
journalctl --list-boots > "$out/boots.txt" 2>&1
journalctl --since "$1" --until "$2" > "$out/journal.log" 2>&1
journalctl -k --since "$1" --until "$2" > "$out/kernel.log" 2>&1

# NiFi logs are in a volume, rotated files are named nifi-app_YYYY-MM-DD_HH.N.log
day=$(date -d "$1" +%Y-%m-%d)
docker run --rm -v big-data-ztm_nifi_logs:/logs -v "$out":/out apache/hadoop:3.4.3 \
  bash -c "ls -la /logs > /out/nifi-logs-list.txt; cp /logs/nifi-app*${day}* /logs/nifi-app.log /out/ 2>/dev/null; true"

echo
echo '--- container state ---'
cat "$out/containers-state.txt"
echo
echo '--- files mentioning OOM / killed ---'
grep -il -E 'OutOfMemory|oom|killed' "$out"/*.log || echo '(none)'
