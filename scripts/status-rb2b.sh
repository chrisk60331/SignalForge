#!/usr/bin/env bash
# status-rb2b.sh — RB2B site visitors panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

rb2b_exists=0
rb2b_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='rb2b_visitors';" 2>/dev/null || echo 0)

total_v=0; identified_v=0; unsent_v=0; visit_first_v=never; visit_last_v=never

if [[ "$rb2b_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'total_v='      || COUNT(*) FROM rb2b_visitors;
SELECT 'identified_v=' || COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '';
SELECT 'unsent_v='     || COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '' AND event_emitted_at IS NULL;
SELECT 'visit_first_v=' || COALESCE(MIN(SUBSTR(source_file, INSTR(source_file,'rb2b_')+5, 10)),'never') FROM rb2b_visitors WHERE source_file LIKE '%rb2b_%';
SELECT 'visit_last_v='  || COALESCE(MAX(SUBSTR(source_file, INSTR(source_file,'rb2b_')+5, 10)),'never') FROM rb2b_visitors WHERE source_file LIKE '%rb2b_%';
SQL
  )" 2>/dev/null || true
fi

echo ""
if [[ "$rb2b_exists" -eq 0 || "$total_v" -eq 0 ]]; then
  echo "    No RB2B data yet — run signalforge-rb2b to import."
else
  printf " %-22s %-5s %-6s %-9s\n" "Visit window" "Total" "Emails" "Outbox"
  printf " %-22s %-5s %-6s %-9s\n" \
    "${visit_first_v}-${visit_last_v}" \
    "$(comma $total_v)" \
    "$(comma $identified_v)" \
    "$(comma $unsent_v)"
fi
