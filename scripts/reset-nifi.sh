#!/usr/bin/env bash
# Hard reset of NiFi queues, e.g. after a crash corrupted the flowfile/content repository.
# Removes only queued data and provenance history. The flow (nifi_conf) and state stay.
# The simulator is NOT stopped: it keeps GPS snapshots in memory and resends them
# once NiFi is back, so stopping it would lose that buffer.

# Needed if the flow is stuck after machine crash

set -euo pipefail
cd "$(dirname "$0")/.."

project=big-data-ztm
volumes=(
  "${project}_nifi_flowfile_repository"
  "${project}_nifi_content_repository"
  "${project}_nifi_provenance_repository"
)

read -r -p "This deletes all NiFi queues (${volumes[*]}). Continue? [y/N] " answer
[[ "$answer" == y ]] || { echo 'Aborted.'; exit 1; }

docker compose stop nifi
docker compose rm -f nifi
docker volume rm "${volumes[@]}"
docker compose up -d nifi

echo 'Waiting for NiFi to start (up to 5 minutes)...'
for (( i=1; i<=60; i++ )); do
  if docker compose logs nifi 2>/dev/null | grep -q 'Started Server'; then
    echo 'NiFi is up. Check the UI for stopped processors or errors.'
    exit 0
  fi
  sleep 5
done
echo 'NiFi did not start within 5 minutes. Check: docker compose logs nifi' >&2
exit 1
