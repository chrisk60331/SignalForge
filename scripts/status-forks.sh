#!/usr/bin/env bash
# status-forks.sh — GitHub forks panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

total_f_repos=0; total_f_final=0; w_email_f_final=0; unsent_f_final=0

eval "$(_sqlite <<'SQL'
SELECT 'total_f_repos='   || COUNT(DISTINCT hackathon_url) FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'total_f_final='   || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'w_email_f_final=' || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:forks:%' AND email != '';
SELECT 'unsent_f_final='  || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:forks:%' AND email != '' AND event_emitted_at IS NULL;
SQL
)" 2>/dev/null || true

echo ""
if [[ "$total_f_final" -eq 0 ]]; then
  echo "    No GitHub fork data yet."
else
  printf " %-8s %-9s %-9s %-9s\n" "Repos" "Total" "Emails" "Outbox"
  printf " %-8s %-9s %-9s %-9s\n" \
    "$(comma $total_f_repos)" \
    "$(comma $total_f_final)" \
    "$(comma $w_email_f_final)" \
    "$(comma $unsent_f_final)"
fi
