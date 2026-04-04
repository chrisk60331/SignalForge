#!/usr/bin/env bash
# status-devto.sh — dev.to challenges panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

devto_exists=0
devto_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='devto_challenges';" 2>/dev/null || echo 0)

total_dt_challenges=0; scraped_dt_challenges=0
total_dt_final=0; w_email_dt_final=0; unsent_dt_final=0

if [[ "$devto_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'total_dt_challenges='   || COUNT(*) FROM devto_challenges;
SELECT 'scraped_dt_challenges=' || COUNT(*) FROM devto_challenges WHERE last_scraped_at IS NOT NULL;
SELECT 'total_dt_final='        || COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%';
SELECT 'w_email_dt_final='      || COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%' AND email != '';
SELECT 'unsent_dt_final='       || COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%' AND email != '' AND event_emitted_at IS NULL;
SQL
  )" 2>/dev/null || true
fi

echo ""
if [[ "$devto_exists" -eq 0 || "$total_dt_challenges" -eq 0 ]]; then
  echo "    No dev.to data yet — run signalforge-devto to scrape."
else
  printf " %-11s %-9s %-9s %-9s\n" "Scraped" "Total" "Emails" "Outbox"
  printf " %-11s %-9s %-9s %-9s\n" \
    "$(comma $scraped_dt_challenges) / $(comma $total_dt_challenges)" \
    "$(comma $total_dt_final)" \
    "$(comma $w_email_dt_final)" \
    "$(comma $unsent_dt_final)"
fi
