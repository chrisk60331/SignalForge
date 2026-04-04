#!/usr/bin/env bash
# status-hackathons.sh — Devpost hackathon + participant counts panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

total_h=0; scraped_h=0
total_p=0; w_email_p=0; unsent_p=0

eval "$(_sqlite <<'SQL'
SELECT 'total_h='   || COUNT(*) FROM hackathons;
SELECT 'scraped_h=' || COUNT(*) FROM hackathons WHERE last_scraped_at IS NOT NULL;
SELECT 'total_p='   || COUNT(*) FROM participants
  WHERE hackathon_url NOT LIKE 'github:forks:%'
    AND hackathon_url NOT LIKE 'github:search:%'
    AND hackathon_url NOT LIKE 'devto:challenge:%'
    AND hackathon_url NOT LIKE 'hn:show%';
SELECT 'w_email_p=' || COUNT(*) FROM participants
  WHERE hackathon_url NOT LIKE 'github:forks:%'
    AND hackathon_url NOT LIKE 'github:search:%'
    AND hackathon_url NOT LIKE 'devto:challenge:%'
    AND hackathon_url NOT LIKE 'hn:show%'
    AND email != '';
SELECT 'unsent_p='  || COUNT(*) FROM participants
  WHERE hackathon_url NOT LIKE 'github:forks:%'
    AND hackathon_url NOT LIKE 'github:search:%'
    AND hackathon_url NOT LIKE 'devto:challenge:%'
    AND hackathon_url NOT LIKE 'hn:show%'
    AND email != '' AND event_emitted_at IS NULL;
SQL
)" 2>/dev/null || true

echo ""
printf " %-13s %-9s %-9s %-9s\n" "Scraped" "Total" "Emails" "Outbox"
printf " %-13s %-9s %-9s %-9s\n" \
  "$(comma $scraped_h) / $(comma $total_h)" \
  "$(comma $total_p)" \
  "$(comma $w_email_p)" \
  "$(comma $unsent_p)"
