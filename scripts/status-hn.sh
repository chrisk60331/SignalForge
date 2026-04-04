#!/usr/bin/env bash
# status-hn.sh — Hacker News Show HN panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

total_hn=0; w_email_hn=0; unsent_hn=0

eval "$(_sqlite <<'SQL'
SELECT 'total_hn='   || COUNT(*) FROM participants WHERE hackathon_url LIKE 'hn:show%';
SELECT 'w_email_hn=' || COUNT(*) FROM participants WHERE hackathon_url LIKE 'hn:show%' AND email != '';
SELECT 'unsent_hn='  || COUNT(*) FROM participants WHERE hackathon_url LIKE 'hn:show%' AND email != '' AND event_emitted_at IS NULL;
SQL
)" 2>/dev/null || true

echo ""
if [[ "$total_hn" -eq 0 ]]; then
  echo "    No HN data yet — run signalforge-hn to scrape."
else
  printf " %-8s %-9s %-9s %-9s\n" "Posts" "Total" "Emails" "Outbox"
  printf " %-8s %-9s %-9s %-9s\n" \
    "$(comma $total_hn)" \
    "$(comma $total_hn)" \
    "$(comma $w_email_hn)" \
    "$(comma $unsent_hn)"
fi
