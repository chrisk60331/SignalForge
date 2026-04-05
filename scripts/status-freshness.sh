#!/usr/bin/env bash
# status-freshness.sh — data staleness (days since last update per source) panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

last_h_days=0; last_f_days=0; last_s_days=0
last_hn_days=0; last_dt_days=0; last_v_days=0
last_h=never; last_f=never; last_s=never
last_hn=never; last_dt=never; last_v=never

rb2b_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='rb2b_visitors';" 2>/dev/null || echo 0)
devto_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='devto_challenges';" 2>/dev/null || echo 0)

eval "$(_sqlite <<'SQL'
SELECT 'last_h='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '), 'localtime'),'never') FROM hackathons;
SELECT 'last_h_days=' || COALESCE(CAST(julianday('now','localtime') - julianday(MAX(DATE(last_scraped_at))) AS INTEGER), 0) FROM hackathons;
SELECT 'last_f='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never') FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'last_f_days=' || COALESCE(CAST(julianday('now','localtime') - julianday(MAX(DATE(last_seen_at))) AS INTEGER), 0) FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'last_s='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never') FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'last_s_days=' || COALESCE(CAST(julianday('now','localtime') - julianday(MAX(DATE(last_seen_at))) AS INTEGER), 0) FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'last_hn='    || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never') FROM participants WHERE hackathon_url LIKE 'hn:show%';
SELECT 'last_hn_days=' || COALESCE(CAST(julianday('now','localtime') - julianday(MAX(DATE(last_seen_at))) AS INTEGER), 0) FROM participants WHERE hackathon_url LIKE 'hn:show%';
SQL
)" 2>/dev/null || true

if [[ "$devto_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'last_dt='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '), 'localtime'),'never') FROM devto_challenges;
SELECT 'last_dt_days=' || COALESCE(CAST(julianday('now','localtime') - julianday(MAX(DATE(last_scraped_at))) AS INTEGER), 0) FROM devto_challenges;
SQL
  )" 2>/dev/null || true
fi

if [[ "$rb2b_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'last_v='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(imported_at),1,19),'T',' '), 'localtime'),'never') FROM rb2b_visitors;
SELECT 'last_v_days=' || COALESCE(CAST(julianday('now','localtime') - julianday(MAX(DATE(imported_at))) AS INTEGER), 0) FROM rb2b_visitors;
SQL
  )" 2>/dev/null || true
fi

echo ""
printf "%-4s %-8s %-6s %-11s\n" "  " "Source" "Days" "Updated"
printf "%-4s %-8s $(color_days $last_v_days) %-11s\n"  "  " "rb2b"    "$last_v"
printf "%-4s %-8s $(color_days $last_dt_days) %-11s\n"  "  " "devto"   "$last_dt"
printf "%-4s %-8s $(color_days $last_hn_days) %-11s\n"  "  " "hn show" "$last_hn"
printf "%-4s %-8s $(color_days $last_f_days) %-11s\n"  "  " "forks"   "$last_f"
printf "%-4s %-8s $(color_days $last_s_days) %-11s\n"  "  " "search"  "$last_s"
printf "%-4s %-8s $(color_days $last_h_days) %-11s\n"  "  " "devpost" "$last_h"
