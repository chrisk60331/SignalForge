#!/usr/bin/env bash
# status-search.sh — GitHub search panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

total_s_queries=0; total_s_final=0; w_email_s_final=0; unsent_s_final=0

eval "$(_sqlite <<'SQL'
SELECT 'total_s_queries=' || COUNT(DISTINCT hackathon_url) FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'total_s_final='   || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'w_email_s_final=' || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:search:%' AND email != '';
SELECT 'unsent_s_final='  || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:search:%' AND email != '' AND event_emitted_at IS NULL;
SQL
)" 2>/dev/null || true

echo ""
if [[ "$total_s_final" -eq 0 ]]; then
  echo "    No GitHub search data yet."
else
  printf " %-9s %-9s %-9s %-9s\n" "Queries" "Total" "Emails" "Outbox"
  printf " %-9s %-9s %-9s %-9s\n" \
    "$(comma $total_s_queries)" \
    "$(comma $total_s_final)" \
    "$(comma $w_email_s_final)" \
    "$(comma $unsent_s_final)"
fi
