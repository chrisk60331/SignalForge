#!/usr/bin/env bash
# status-runs.sh — recent signalforge-run history panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

runs_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='runs';" 2>/dev/null || echo 0)

if [[ "${runs_exists:-0}" -eq 0 ]]; then
  echo "    No run history yet."
else
  echo ""
  printf "%-8s %-8s %-8s  %s\n" "" "Command" "Status" "Time"
  while IFS='|' read -r raw_cmd raw_status raw_ts; do
    label="${raw_cmd#signalforge-}"
    label="${label:0:16}"
    ts="${raw_ts:11:5}"
    printf "%-8s %-8s %-8s  %s\n" "" "$label" "$(color_status "$raw_status")" "${ts}"
  done < <(_sqlite "
SELECT command, status, REPLACE(started_at,'T',' ')
FROM runs ORDER BY id DESC LIMIT 6;
" 2>/dev/null)
fi
