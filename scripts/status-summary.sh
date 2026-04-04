#!/usr/bin/env bash
# status-summary.sh — cross-source totals + Customer.io campaign metrics panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

rb2b_exists=$(_sqlite "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='rb2b_visitors';" 2>/dev/null || echo 0)

# ── Devpost + forks + devto participant counts ────────────────────────────────
p_leads=0; p_unsent=0; emitted_today_p=0
total_f_final=0; total_p=0; total_dt_final=0
last_h=never; last_p=never; last_v=never

eval "$(_sqlite <<'SQL'
SELECT 'p_leads='  || COUNT(*) FROM participants WHERE email != '';
SELECT 'p_unsent=' || COUNT(*) FROM participants WHERE email != '' AND event_emitted_at IS NULL;
SELECT 'emitted_today_p=' || COUNT(*) FROM participants
  WHERE DATE(strftime('%Y-%m-%d', REPLACE(SUBSTR(event_emitted_at,1,19),'T',' '), 'localtime')) = DATE('now','localtime');
SELECT 'total_f_final=' || COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:forks:%';
SELECT 'total_p='       || COUNT(*) FROM participants
  WHERE hackathon_url NOT LIKE 'github:forks:%'
    AND hackathon_url NOT LIKE 'github:search:%'
    AND hackathon_url NOT LIKE 'devto:challenge:%'
    AND hackathon_url NOT LIKE 'hn:show%';
SELECT 'total_dt_final=' || COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%';
SELECT 'last_h=' || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '),'localtime'),'never') FROM hackathons;
SELECT 'last_p=' || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '),'localtime'),'never')    FROM participants;
SQL
)" 2>/dev/null || true

# ── RB2B counts ───────────────────────────────────────────────────────────────
r_leads=0; r_unsent=0; emitted_today_v=0

if [[ "$rb2b_exists" -eq 1 ]]; then
  eval "$(_sqlite <<'SQL'
SELECT 'r_leads='       || COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '';
SELECT 'r_unsent='      || COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '' AND event_emitted_at IS NULL;
SELECT 'emitted_today_v=' || COUNT(*) FROM rb2b_visitors
  WHERE DATE(strftime('%Y-%m-%d', REPLACE(SUBSTR(event_emitted_at,1,19),'T',' '), 'localtime')) = DATE('now','localtime');
SELECT 'last_v=' || COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(imported_at),1,19),'T',' '),'localtime'),'never') FROM rb2b_visitors;
SQL
  )" 2>/dev/null || true
fi

total_leads=$(( p_leads + r_leads ))
total_unsent=$(( p_unsent + r_unsent ))
total_scraped=$(( total_f_final + total_p + total_dt_final ))
emitted_today=$(( emitted_today_p + emitted_today_v ))

last_scraped=""
for _ts in "${last_h:-never}" "${last_p:-never}" "${last_v:-never}"; do
  [[ "$_ts" != "never" && ( -z "$last_scraped" || "$_ts" > "$last_scraped" ) ]] && last_scraped="$_ts"
done
[[ -z "$last_scraped" ]] && last_scraped="never"

header "📊  Summary"
printf "    %-32s %-9s %-9s %-9s %-20s\n" "Emails sent today" "Total" "Emails" "Outbox" "Last Updated"
printf "    %-32s %-9s %-9s %-9s %-20s\n" \
  "$(comma $emitted_today)" \
  "$(comma $total_scraped)" \
  "$(comma $total_leads)" \
  "$(comma $total_unsent)" \
  "$last_scraped"

# ── Customer.io campaign metrics ──────────────────────────────────────────────
ENV_FILE="${PROJECT_DIR}/.env"
[[ -f "$ENV_FILE" ]] && set -a && source "$ENV_FILE" && set +a

CIO_APP_KEY="${CUSTOMERIO_APP_API_KEY:-}"
if [[ -n "$CIO_APP_KEY" ]]; then
  header "📬  Customer.io — Campaigns"

  CIO_RESP=$(curl -s --max-time 8 \
    -H "Authorization: Bearer $CIO_APP_KEY" \
    "https://api.customer.io/v1/campaigns?limit=20")
  HTTP_ERR=$(echo "$CIO_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('errors',''))" 2>/dev/null)

  if [[ -n "$HTTP_ERR" && "$HTTP_ERR" != "[]" && "$HTTP_ERR" != "" ]]; then
    echo "    API error: $HTTP_ERR"
  else
    CIO_JSON="$CIO_RESP" CIO_APP_KEY="$CIO_APP_KEY" python3 - <<'PYEOF'
import os, json
from concurrent.futures import ThreadPoolExecutor
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
        pct       = f"{converted/delivered*100:.1f}%" if delivered else "—"
        conv_col  = f"{converted:,} ({pct})"
        print(fmt.format(name, f"{sent:,}", f"{delivered:,}", f"{clicked:,}", conv_col, f"{unsubs:,}"))
    if len(campaigns) > 10:
        print(f"\n    ... and {len(campaigns) - 10:,} more")
PYEOF
  fi
fi
