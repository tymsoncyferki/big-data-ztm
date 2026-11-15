#!/usr/bin/env bash
# Compatibility wrapper; scheduling is now Docker-contained, not host cron.
set -euo pipefail
exec bash "$(dirname "$0")/start-schedule.sh" "$@"
