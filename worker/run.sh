#!/usr/bin/env bash
set -euo pipefail

INTERVAL_MINUTES="${INTERVAL_MINUTES:-360}"
echo "[worker] interval=${INTERVAL_MINUTES}m"

while true; do
  echo "[worker] run at $(date -Is)"
  /venv/bin/python -m cineplexx_rss.main
  echo "[worker] sleep ${INTERVAL_MINUTES}m"
  sleep "$((INTERVAL_MINUTES * 60))"
done