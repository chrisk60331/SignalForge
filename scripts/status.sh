#!/usr/bin/env bash
# SignalForge — harvest status dashboard
# Usage: ./scripts/status.sh [path/to/devpost_harvest.db] [--section <name>]
#
# Sections: hackathons | forks | search | devto | rb2b | summary | cio | sf | all (default)
# WTF dashboard: wtf -c scripts/wtf.yml

set -uo pipefail

trap 'echo ""; exit 1' INT TERM SIGINT SIGTERM

# Parse args — DB path can appear in any position; --section is named
DB="devpost_harvest.db"
SECTION="all"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --section) SECTION="$2"; shift 2 ;;
    --section=*) SECTION="${1#*=}"; shift ;;
    *) DB="$1"; shift ;;
  esac
done

# Run sqlite3 in read-only mode with a 5 s busy-timeout so watch -n2 never
# crashes on a locked DB.  Falls back up to MAX_RETRIES times with a short
# sleep before giving up.
MAX_RETRIES=5
_sqlite() {
  local attempt=0
  local out rc
  while (( attempt < MAX_RETRIES )); do
    out=$(sqlite3 --readonly -cmd ".timeout 5000" "$DB" "$@" 2>&1)
    rc=$?
    if [[ $rc -eq 0 ]]; then
      echo "$out"
      return 0
    fi
    if echo "$out" | grep -qi "database is locked"; then
      (( attempt++ ))
      sleep 0.3
    else
      echo "$out" >&2
      return $rc
    fi
  done
  echo "  ⚠  DB still locked after ${MAX_RETRIES} retries — will refresh next tick" >&2
  return 1
}

if [[ ! -f "$DB" ]]; then
  echo "Database not found: $DB"
  echo "Run signalforge-harvest first, or pass a path: $0 path/to/harvest.db"
  exit 1
fi

header() { printf "\n  %s\n  %s\n" "$1" "$(printf -- '-%.0s' {1..54})"; }
row()    { printf "    %-32s  %s\n" "$1" "$2"; }
sep()    { printf "    %s\n" "$(printf '.%.0s' {1..52})"; }
comma()  {
  awk -v n="${1:-0}" 'BEGIN {
    while (n > 999) { r = "," sprintf("%03d", n % 1000) r; n = int(n / 1000) }
    print n r
  }'
}


# ── Single bulk query — all scalar stats + rb2b table check ───────────────────
total_h=0; scraped_h=0; last_h=never
total_p=0; w_email_p=0; emitted_p=0; unsent_p=0; last_p=never
p_leads=0; p_unsent=0; emitted_today_p=0; rb2b_exists=0; devto_exists=0

eval "$(_sqlite <<'SQL'
SELECT 'total_h='    || COUNT(*)                                                                                      FROM hackathons;
SELECT 'scraped_h='  || COUNT(*)                                                                                      FROM hackathons WHERE last_scraped_at IS NOT NULL;
SELECT 'last_h='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '), 'localtime'),'never')  FROM hackathons;
SELECT 'last_h_days=' || (CURRENT_DATE - MAX(last_scraped_at)) FROM hackathons;
SELECT 'total_p='    || COUNT(*)  FROM participants WHERE hackathon_url NOT LIKE 'github:forks:%' AND hackathon_url NOT LIKE 'github:search:%' AND hackathon_url NOT LIKE 'devto:challenge:%';
SELECT 'w_email_p='  || COUNT(*)  FROM participants WHERE hackathon_url NOT LIKE 'github:forks:%' AND hackathon_url NOT LIKE 'github:search:%' AND hackathon_url NOT LIKE 'devto:challenge:%' AND email != '';
SELECT 'emitted_p='  || COUNT(*)  FROM participants WHERE hackathon_url NOT LIKE 'github:forks:%' AND hackathon_url NOT LIKE 'github:search:%' AND hackathon_url NOT LIKE 'devto:challenge:%' AND event_emitted_at IS NOT NULL;
SELECT 'unsent_p='   || COUNT(*)  FROM participants WHERE hackathon_url NOT LIKE 'github:forks:%' AND hackathon_url NOT LIKE 'github:search:%' AND hackathon_url NOT LIKE 'devto:challenge:%' AND email != '' AND event_emitted_at IS NULL;
SELECT 'last_p='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never')  FROM participants WHERE hackathon_url NOT LIKE 'github:forks:%' AND hackathon_url NOT LIKE 'github:search:%' AND hackathon_url NOT LIKE 'devto:challenge:%';

SELECT 'p_leads='    || COUNT(*)  FROM participants WHERE email != '';
SELECT 'p_unsent='   || COUNT(*)  FROM participants WHERE email != '' AND event_emitted_at IS NULL;

SELECT 'emitted_today_p=' || COUNT(*) FROM participants WHERE DATE(strftime('%Y-%m-%d', REPLACE(SUBSTR(event_emitted_at,1,19),'T',' '), 'localtime')) = DATE('now','localtime');

SELECT 'rb2b_exists='  || COUNT(*) FROM sqlite_master WHERE type='table' AND name='rb2b_visitors';
SELECT 'devto_exists=' || COUNT(*) FROM sqlite_master WHERE type='table' AND name='devto_challenges';
SQL
)" 2>/dev/null || true

# ── Conditional second query — rb2b stats (only if table exists) ──────────────
total_v=0; identified_v=0; w_linkedin_v=0; emitted_v=0; unsent_v=0; last_v=never; visit_first_v=never; visit_last_v=never
emitted_today_v=0

if [[ -n "${rb2b_exists+set}" && -n "$rb2b_exists" && "$rb2b_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'total_v='      || COUNT(*)                                            FROM rb2b_visitors;
SELECT 'identified_v=' || COUNT(*)                                            FROM rb2b_visitors WHERE email IS NOT NULL AND email != '';
SELECT 'w_linkedin_v=' || COUNT(*)                                            FROM rb2b_visitors WHERE linkedin_url IS NOT NULL AND linkedin_url != '';
SELECT 'emitted_v='    || COUNT(*)                                            FROM rb2b_visitors WHERE event_emitted_at IS NOT NULL;
SELECT 'unsent_v='     || COUNT(*)                                            FROM rb2b_visitors WHERE email IS NOT NULL AND email != '' AND event_emitted_at IS NULL;
SELECT 'last_v='          || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(imported_at),1,19),'T',' '), 'localtime'),'never')  FROM rb2b_visitors;
SELECT 'emitted_today_v=' || COUNT(*) FROM rb2b_visitors WHERE DATE(strftime('%Y-%m-%d', REPLACE(SUBSTR(event_emitted_at,1,19),'T',' '), 'localtime')) = DATE('now','localtime');
SELECT 'visit_first_v='   || COALESCE(MIN(SUBSTR(source_file, INSTR(source_file,'rb2b_')+5, 10)),'never') FROM rb2b_visitors WHERE source_file LIKE '%rb2b_%';
SELECT 'visit_last_v='    || COALESCE(MAX(SUBSTR(source_file, INSTR(source_file,'rb2b_')+5, 10)),'never') FROM rb2b_visitors WHERE source_file LIKE '%rb2b_%';
SELECT 'last_v_days='       || (CURRENT_DATE - MAX(imported_at)) FROM rb2b_visitors;
SQL
)" 2>/dev/null || true
fi


# ── Fork scalar query ──────────────────────────────────────────────────────────
total_f_repos=0; total_f_final=0; w_email_f_final=0; unsent_f_final=0; last_f=never
eval "$(_sqlite <<'SQL'
SELECT 'total_f_repos='   || COUNT(DISTINCT hackathon_url) FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'total_f_final='   || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'w_email_f_final=' || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:forks:%' AND email != '';
SELECT 'unsent_f_final='  || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:forks:%' AND email != '' AND event_emitted_at IS NULL;
SELECT 'last_f='          || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never') FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'last_f_days='       || (CURRENT_DATE - MAX(last_seen_at)) FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SQL
)" 2>/dev/null || true

# ── Search scalar query ────────────────────────────────────────────────────────
total_s_queries=0; total_s_final=0; w_email_s_final=0; unsent_s_final=0; last_s=never
eval "$(_sqlite <<'SQL'
SELECT 'total_s_queries='  || COUNT(DISTINCT hackathon_url) FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'total_s_final='    || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'w_email_s_final='  || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:search:%' AND email != '';
SELECT 'unsent_s_final='   || COUNT(*)                      FROM participants WHERE hackathon_url LIKE 'github:search:%' AND email != '' AND event_emitted_at IS NULL;
SELECT 'last_s='           || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never') FROM participants WHERE hackathon_url LIKE 'github:search:%';
SELECT 'last_s_days='       || (CURRENT_DATE - MAX(last_seen_at)) FROM participants WHERE hackathon_url LIKE 'github:search:%';
SQL
)" 2>/dev/null || true

# ── HN Show scalar query ───────────────────────────────────────────────────────
total_hn=0; w_email_hn=0; unsent_hn=0; last_hn=never; last_hn_days=0
eval "$(_sqlite <<'SQL'
SELECT 'total_hn='    || COUNT(*)   FROM participants WHERE hackathon_url LIKE 'hn:show%';
SELECT 'w_email_hn='  || COUNT(*)   FROM participants WHERE hackathon_url LIKE 'hn:show%' AND email != '';
SELECT 'unsent_hn='   || COUNT(*)   FROM participants WHERE hackathon_url LIKE 'hn:show%' AND email != '' AND event_emitted_at IS NULL;
SELECT 'last_hn='     || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '), 'localtime'),'never') FROM participants WHERE hackathon_url LIKE 'hn:show%';
SELECT 'last_hn_days=' || COALESCE((CURRENT_DATE - MAX(DATE(last_seen_at))), 0) FROM participants WHERE hackathon_url LIKE 'hn:show%';
SQL
)" 2>/dev/null || true

# ── dev.to challenge scalar query ─────────────────────────────────────────────
total_dt_challenges=0; scraped_dt_challenges=0
total_dt_final=0; w_email_dt_final=0; unsent_dt_final=0; last_dt=never

if [[ "$devto_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'total_dt_challenges='   || COUNT(*)   FROM devto_challenges;
SELECT 'scraped_dt_challenges=' || COUNT(*)   FROM devto_challenges WHERE last_scraped_at IS NOT NULL;
SELECT 'total_dt_final='        || COUNT(*)   FROM participants WHERE hackathon_url LIKE 'devto:challenge:%';
SELECT 'w_email_dt_final='      || COUNT(*)   FROM participants WHERE hackathon_url LIKE 'devto:challenge:%' AND email != '';
SELECT 'unsent_dt_final='       || COUNT(*)   FROM participants WHERE hackathon_url LIKE 'devto:challenge:%' AND email != '' AND event_emitted_at IS NULL;
SELECT 'last_dt='               || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '), 'localtime'),'never') FROM devto_challenges;
SELECT 'last_dt_days='          || (CURRENT_DATE - MAX(last_scraped_at)) FROM devto_challenges;
SQL
  )" 2>/dev/null || true
fi



# ── Banner (full-output mode only) ────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "banner" ]]; then
  cat <<'BANNER'

                         _____ _                   ________
                        / ___/(_)___ _____  ____ _/ / ____/___  _________ ____
                        \__ \/ / __ `/ __ \/ __ `/ / /_  / __ \/ ___/ __ `/ _ \
                       ___/ / / /_/ / / / / /_/ / / __/ / /_/ / /  / /_/ /  __/
                      /____/_/\__, /_/ /_/\__,_/_/_/    \____/_/   \__, /\___/
                             /____/                               /____/
BANNER
  SCRIPT_DIR_BANNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  SF_VERSION=$(grep '^version' "$SCRIPT_DIR_BANNER/../pyproject.toml" 2>/dev/null \
    | head -1 | sed 's/version *= *"\(.*\)"/\1/')
  echo ""
  printf "    %-44s %-6s %-6s %-7s %-6s \n" "DB: ${DB}"   "v${SF_VERSION:-?}"
fi
if [[ "$SECTION" == "all" || "$SECTION" == "hn" ]]; then
  header "🟠  HN Show — GitHub Posts"

  if [[ "$total_hn" -eq 0 ]]; then
    echo "    No HN data yet — run signalforge-hn to scrape."
  else
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "Posts" "Total" "Emails" "Outbox" "Last Updated"
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "$(comma $total_hn)" "$(comma $total_hn)" "$(comma $w_email_hn)" "$(comma $unsent_hn)" "$last_hn"
  fi
fi
if [[ "$SECTION" == "all" || "$SECTION" == "runs" ]]; then
  color_status() {
    case "$1" in
      running)     printf "\033[33m%s\033[0m" "$1" ;;
      done)        printf "\033[32m%s\033[0m" "$1" ;;
      failed)      printf "\033[31m%s\033[0m" "$1" ;;
      interrupted) printf "\033[35m%s\033[0m" "$1" ;;
      *)           printf "%s" "$1" ;;
    esac
  }

  runs_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='runs';" 2>/dev/null || echo 0)

  if [[ "${runs_exists:-0}" -eq 0 ]]; then
    echo "    No run history yet."
  else
    while IFS='|' read -r raw_cmd raw_status raw_ts; do
      label="${raw_cmd#signalforge-}"
      label="${label:0:16}"
      ts="${raw_ts:11:5}"
      printf "          %-16s  %-11s  %s\n" "$label" "$(color_status "$raw_status")" "$ts"
    done < <(_sqlite "
SELECT command, status, REPLACE(started_at,'T',' ')
FROM runs ORDER BY id DESC LIMIT 6;
" 2>/dev/null)
  fi
fi
# ── Render: Devpost hackathons ────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "freshness" ]]; then
  color_days() {
    if [[ "$1" -gt 1 ]]; then
      printf "\033[31m$1\033[0m" 
    elif [[ "$1" -eq 0 ]]; then
      printf "\033[32m%s\033[0m" "$1"
    else
      printf "\033[33m%s\033[0m" "$1"
    fi
  }
  header "  Staleness"
  printf "          %-10s $(color_days $last_v_days)\n" "rb2b"
  printf "          %-10s $(color_days $last_dt_days)\n" "devto"
  printf "          %-10s $(color_days $last_hn_days)\n" "hn show"
  printf "          %-10s $(color_days $last_f_days)\n" "forks"
  printf "          %-10s $(color_days $last_s_days)\n" "search"
  printf "          %-10s $(color_days $last_h_days)\n" "devpost"
 fi

# ── Render: Devpost hackathons ────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "hackathons" ]]; then
  header "🏆  Devpost — Hackathons"
  printf "    %-32s %-9s %-9s %-9s %-11s \n"    "Scraped" "Total" "Emails" "Outbox"  "Last Updated"
  printf "    %-32s %-9s %-9s %-9s %-11s \n"   "$(comma $scraped_h) / $(comma $total_h)" "$(comma $total_p)" "$(comma $w_email_p)" "$(comma $unsent_p)"  "$last_h"
fi


# ── Render: GitHub forks ──────────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "forks" ]]; then
  header "🍴  GitHub Forks"

  if [[ "$total_f_final" -eq 0 ]]; then
    echo "    No GitHub fork data yet."
  else
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "Repos" "Total" "Emails" "Outbox" "Last Updated"
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "$(comma $total_f_repos)" "$(comma $total_f_final)" "$(comma $w_email_f_final)" "$(comma $unsent_f_final)" "$last_f"
  fi
fi


# ── Render: GitHub search ─────────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "search" ]]; then
  header "🔍  GitHub Search"

  if [[ "$total_s_final" -eq 0 ]]; then
    echo "    No GitHub search data yet."
  else
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "Queries" "Total" "Emails" "Outbox" "Last Updated"
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "$(comma $total_s_queries)" "$(comma $total_s_final)" "$(comma $w_email_s_final)" "$(comma $unsent_s_final)" "$last_s"
  fi
fi


# ── Render: dev.to challenges ─────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "devto" ]]; then
  header "🟣  dev.to — Challenges"

  if [[ "$devto_exists" -eq 0 || "$total_dt_challenges" -eq 0 ]]; then
    echo "    No dev.to data yet — run signalforge-devto to scrape."
  else
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "Scraped" "Total" "Emails" "Outbox" "Last Updated"
    printf "    %-32s %-9s %-9s %-9s %-11s \n" "$(comma $scraped_dt_challenges) / $(comma $total_dt_challenges)" "$(comma $total_dt_final)" "$(comma $w_email_dt_final)" "$(comma $unsent_dt_final)" "$last_dt"
  fi
fi


# ── Render: RB2B visitors ─────────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "rb2b" ]]; then
  header "👁   RB2B — Visitors"

  if [[ -n "${rb2b_exists+set}" && "$rb2b_exists" -eq 0 ]]; then
    echo "    No RB2B data yet — run signalforge-rb2b to import."
  elif [[ "$total_v" -eq 0 ]]; then
    echo "    No RB2B data yet — run signalforge-rb2b to import."
  else
    printf "    %-32s %-9s %-9s %-9s %-33s \n"  "Visit window" "Total"     "Emails"        "Outbox"    "Last imported at"
    printf "    %-34s %-9s %-9s %-9s %-33s \n" "${visit_first_v} → ${visit_last_v}"  "$(comma $total_v)" "$(comma $identified_v)" "$(comma $unsent_v)" "$last_v"
  fi
fi


# ── Render: Summary ───────────────────────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "summary" ]]; then
  header "📊  Summary"

  if [[ -n "${rb2b_exists+set}" && "$rb2b_exists" -eq 1 ]]; then
    r_leads=$identified_v
    r_unsent=$unsent_v
  fi

  if [[ -n "${p_leads+set}" && -n "${r_leads+set}" && -n "${p_unsent+set}" && -n "${r_unsent+set}" ]]; then
    total_leads=$(( p_leads + r_leads ))
    total_unsent=$(( p_unsent + r_unsent ))
    sdufishdfu=$((total_f_final + total_p + ${total_dt_final:-0}))
    emitted_today=$(( ${emitted_today_p:-0} + ${emitted_today_v:-0} ))

    last_scraped=""
    for _ts in "${last_h:-never}" "${last_p:-never}" "${last_v:-never}"; do
      [[ "$_ts" != "never" && ( -z "$last_scraped" || "$_ts" > "$last_scraped" ) ]] && last_scraped="$_ts"
    done
    [[ -z "$last_scraped" ]] && last_scraped="never"

    printf "    %-32s %-9s %-9s %-9s %-6s \n" "Emails sent today"  "Total" "Emails" "Outbox" "Last Updated"
    printf "    %-32s %-9s %-9s %-9s %-6s \n" "$(comma $emitted_today)"  "$(comma $sdufishdfu)" "$(comma $total_leads)" "$(comma $total_unsent)" "$last_scraped"
  else
    row "Total leads with email"    "Connecting to database..."
    row "Total unsent events"       "Connecting to database..."
  fi

  SCRIPT_DIR_CIO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ENV_FILE="$SCRIPT_DIR_CIO/../.env"
  [[ -f "$ENV_FILE" ]] && set -a && source "$ENV_FILE" && set +a

  CIO_APP_KEY="${CUSTOMERIO_APP_API_KEY:-}"
  if [[ -n "$CIO_APP_KEY" ]]; then
    header "📬  Customer.io — Campaigns"

    CIO_RESP=$(curl -s --max-time 8 \
      -H "Authorization: Bearer $CIO_APP_KEY" \
      "https://api.customer.io/v1/campaigns?limit=20" )
    HTTP_ERR=$(echo "$CIO_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('errors',''))" 2>/dev/null)

    if [[ -n "$HTTP_ERR" && "$HTTP_ERR" != "[]" && "$HTTP_ERR" != "" ]]; then
      echo "    API error: $HTTP_ERR"
    else
      CIO_JSON="$CIO_RESP" CIO_APP_KEY="$CIO_APP_KEY" python3 - <<'PYEOF'
import os, json
from urllib.request import Request, urlopen

key      = os.environ['CIO_APP_KEY']
data     = json.loads(os.environ['CIO_JSON'])
campaigns = data.get("campaigns", [])
campaigns.sort(key=lambda c: c.get("updated", 0), reverse=True)

def fetch_metrics(cid):
    url = f"https://api.customer.io/v1/campaigns/{cid}/metrics"
    req = Request(url, headers={"Authorization": f"Bearer {key}"})
    try:
        with urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
        s = d.get("metric", {}).get("series", {})
        return {k: sum(v) for k, v in s.items()}
    except Exception:
        return {}

if not campaigns:
    print("    No campaigns found.")
else:
    from concurrent.futures import ThreadPoolExecutor
    ids = [c["id"] for c in campaigns[:10]]
    with ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(fetch_metrics, ids))
    metrics = dict(zip(ids, results))

    fmt = "    {:<29}  {:>8}  {:>8}  {:>7}  {:>11}  {:>6}"
    print(fmt.format("Campaign", "Sent", "Deliv", "Click", "Convert", "Unsubs"))
    for c in campaigns[:10]:
        name = c.get("name", "—")[:27]
        m = metrics.get(c["id"], {})
        sent      = m.get("sent", 0)
        delivered = m.get("delivered", 0)
        clicked   = m.get("human_clicked", m.get("clicked", 0))
        converted = m.get("converted", 0)
        unsubs    = m.get("unsubscribed", 0)
        pct = f"{converted/delivered*100:.1f}%" if delivered else "—"
        conv_col  = f"{converted:,} ({pct})"
        print(fmt.format(name, f"{sent:,}", f"{delivered:,}", f"{clicked:,}", conv_col, f"{unsubs:,}"))
    if len(campaigns) > 10:
        print(f"\n    ... and {len(campaigns) - 10:,} more")
PYEOF
    fi
  fi
fi


# ── Render: SignalForge live status ───────────────────────────────────────────
if [[ "$SECTION" == "all" || "$SECTION" == "git" ]]; then
  git status -s
fi
