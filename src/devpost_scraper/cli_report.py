"""signalforge-report — build and email an hourly HTML dashboard report.

Replicates every WTF-dashboard panel as a delightful HTML email and sends it
via Gmail (GMAIL_USER + GMAIL_APP_PASSWORD) or prints it dry-run style.

Usage:
    signalforge-report                       # send to GMAIL_USER
    signalforge-report --to me@example.com  # explicit recipient
    signalforge-report --dry-run            # print HTML, do not send
    signalforge-report --db /path/to/db     # custom DB
"""

import argparse
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.request import Request, urlopen

from dotenv import load_dotenv

from devpost_scraper.gmail_sender import SendEmailRequest, send_email

# ── SQLite helpers ─────────────────────────────────────────────────────────────


def _q(db_path: str, sql: str) -> list:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()
    except Exception:
        return []


def _scalar(db_path: str, sql: str, default=0):
    rows = _q(db_path, sql)
    if rows:
        val = rows[0][0]
        return val if val is not None else default
    return default


def _table_exists(db_path: str, table: str) -> bool:
    return bool(
        _scalar(db_path, f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}';", 0)
    )


def _fresh(db_path: str, sql: str) -> tuple[str, int]:
    """Return (timestamp_str, days_old) from a freshness query."""
    rows = _q(db_path, sql)
    if rows and rows[0][0] and str(rows[0][0]) != "never":
        # rows[0][1] can be 0 (today) — must not use `or`, which treats 0 as falsy
        raw = rows[0][1]
        return str(rows[0][0]), int(raw if raw is not None else 99)
    return "never", 99


# ── Data collection ────────────────────────────────────────────────────────────

_DEVPOST_NOT = """
    hackathon_url NOT LIKE 'github:forks:%'
    AND hackathon_url NOT LIKE 'github:search:%'
    AND hackathon_url NOT LIKE 'devto:challenge:%'
    AND hackathon_url NOT LIKE 'hn:show%'
"""


def _collect(db_path: str) -> dict:
    d: dict = {}

    # Hackathons (Devpost)
    d["total_h"] = _scalar(db_path, "SELECT COUNT(*) FROM hackathons;")
    d["scraped_h"] = _scalar(db_path, "SELECT COUNT(*) FROM hackathons WHERE last_scraped_at IS NOT NULL;")
    d["total_p"] = _scalar(db_path, f"SELECT COUNT(*) FROM participants WHERE {_DEVPOST_NOT};")
    d["w_email_p"] = _scalar(db_path, f"SELECT COUNT(*) FROM participants WHERE {_DEVPOST_NOT} AND email != '';")
    d["unsent_p"] = _scalar(db_path, f"SELECT COUNT(*) FROM participants WHERE {_DEVPOST_NOT} AND email != '' AND event_emitted_at IS NULL;")

    # HN
    d["total_hn"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'hn:show%';")
    d["w_email_hn"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'hn:show%' AND email != '';")
    d["unsent_hn"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'hn:show%' AND email != '' AND event_emitted_at IS NULL;")

    # GitHub Forks
    d["total_f_repos"] = _scalar(db_path, "SELECT COUNT(DISTINCT hackathon_url) FROM participants WHERE hackathon_url LIKE 'github:forks:%';")
    d["total_f"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:forks:%';")
    d["w_email_f"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:forks:%' AND email != '';")
    d["unsent_f"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:forks:%' AND email != '' AND event_emitted_at IS NULL;")

    # GitHub Search
    d["total_s_queries"] = _scalar(db_path, "SELECT COUNT(DISTINCT hackathon_url) FROM participants WHERE hackathon_url LIKE 'github:search:%';")
    d["total_s"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:search:%';")
    d["w_email_s"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:search:%' AND email != '';")
    d["unsent_s"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'github:search:%' AND email != '' AND event_emitted_at IS NULL;")

    # dev.to
    d["devto_exists"] = _table_exists(db_path, "devto_challenges")
    if d["devto_exists"]:
        d["total_dt_challenges"] = _scalar(db_path, "SELECT COUNT(*) FROM devto_challenges;")
        d["scraped_dt_challenges"] = _scalar(db_path, "SELECT COUNT(*) FROM devto_challenges WHERE last_scraped_at IS NOT NULL;")
        d["total_dt"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%';")
        d["w_email_dt"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%' AND email != '';")
        d["unsent_dt"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE hackathon_url LIKE 'devto:challenge:%' AND email != '' AND event_emitted_at IS NULL;")
    else:
        d["total_dt_challenges"] = d["scraped_dt_challenges"] = d["total_dt"] = d["w_email_dt"] = d["unsent_dt"] = 0

    # RB2B
    d["rb2b_exists"] = _table_exists(db_path, "rb2b_visitors")
    if d["rb2b_exists"]:
        d["total_v"] = _scalar(db_path, "SELECT COUNT(*) FROM rb2b_visitors;")
        d["identified_v"] = _scalar(db_path, "SELECT COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '';")
        d["unsent_v"] = _scalar(db_path, "SELECT COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '' AND event_emitted_at IS NULL;")
        rows = _q(
            db_path,
            "SELECT MIN(SUBSTR(source_file,INSTR(source_file,'rb2b_')+5,10)), MAX(SUBSTR(source_file,INSTR(source_file,'rb2b_')+5,10)) FROM rb2b_visitors WHERE source_file LIKE '%rb2b_%';",
        )
        d["visit_first_v"] = rows[0][0] if rows and rows[0][0] else "—"
        d["visit_last_v"] = rows[0][1] if rows and rows[0][1] else "—"
    else:
        d["total_v"] = d["identified_v"] = d["unsent_v"] = 0
        d["visit_first_v"] = d["visit_last_v"] = "—"

    # Freshness (last updated per source)
    d["last_h"], d["last_h_days"] = _fresh(db_path, """
        SELECT COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '),'localtime'),'never'),
               COALESCE(CAST(julianday('now','localtime')-julianday(MAX(DATE(last_scraped_at))) AS INTEGER),99)
        FROM hackathons;
    """)
    d["last_f"], d["last_f_days"] = _fresh(db_path, """
        SELECT COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '),'localtime'),'never'),
               COALESCE(CAST(julianday('now','localtime')-julianday(MAX(DATE(last_seen_at))) AS INTEGER),99)
        FROM participants WHERE hackathon_url LIKE 'github:forks:%';
    """)
    d["last_s"], d["last_s_days"] = _fresh(db_path, """
        SELECT COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '),'localtime'),'never'),
               COALESCE(CAST(julianday('now','localtime')-julianday(MAX(DATE(last_seen_at))) AS INTEGER),99)
        FROM participants WHERE hackathon_url LIKE 'github:search:%';
    """)
    d["last_hn"], d["last_hn_days"] = _fresh(db_path, """
        SELECT COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_seen_at),1,19),'T',' '),'localtime'),'never'),
               COALESCE(CAST(julianday('now','localtime')-julianday(MAX(DATE(last_seen_at))) AS INTEGER),99)
        FROM participants WHERE hackathon_url LIKE 'hn:show%';
    """)
    if d["devto_exists"]:
        d["last_dt"], d["last_dt_days"] = _fresh(db_path, """
            SELECT COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(last_scraped_at),1,19),'T',' '),'localtime'),'never'),
                   COALESCE(CAST(julianday('now','localtime')-julianday(MAX(DATE(last_scraped_at))) AS INTEGER),99)
            FROM devto_challenges;
        """)
    else:
        d["last_dt"], d["last_dt_days"] = "never", 99

    if d["rb2b_exists"]:
        d["last_v"], d["last_v_days"] = _fresh(db_path, """
            SELECT COALESCE(strftime('%Y-%m-%dT%H:%M', REPLACE(SUBSTR(MAX(imported_at),1,19),'T',' '),'localtime'),'never'),
                   COALESCE(CAST(julianday('now','localtime')-julianday(MAX(DATE(imported_at))) AS INTEGER),99)
            FROM rb2b_visitors;
        """)
    else:
        d["last_v"], d["last_v_days"] = "never", 99

    # Summary totals (cross-source)
    d["p_leads"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE email != '';")
    d["p_unsent"] = _scalar(db_path, "SELECT COUNT(*) FROM participants WHERE email != '' AND event_emitted_at IS NULL;")
    d["emitted_today_p"] = _scalar(db_path, """
        SELECT COUNT(*) FROM participants
        WHERE DATE(strftime('%Y-%m-%d',REPLACE(SUBSTR(event_emitted_at,1,19),'T',' '),'localtime'))=DATE('now','localtime');
    """)

    if d["rb2b_exists"]:
        d["r_leads"] = _scalar(db_path, "SELECT COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '';")
        d["r_unsent"] = _scalar(db_path, "SELECT COUNT(*) FROM rb2b_visitors WHERE email IS NOT NULL AND email != '' AND event_emitted_at IS NULL;")
        d["emitted_today_v"] = _scalar(db_path, """
            SELECT COUNT(*) FROM rb2b_visitors
            WHERE DATE(strftime('%Y-%m-%d',REPLACE(SUBSTR(event_emitted_at,1,19),'T',' '),'localtime'))=DATE('now','localtime');
        """)
    else:
        d["r_leads"] = d["r_unsent"] = d["emitted_today_v"] = 0

    d["total_leads"] = d["p_leads"] + d["r_leads"]
    d["total_unsent"] = d["p_unsent"] + d["r_unsent"]
    d["emitted_today"] = d["emitted_today_p"] + d["emitted_today_v"]

    # Recent runs
    d["runs"] = []
    if _table_exists(db_path, "runs"):
        rows = _q(db_path, "SELECT command, status, REPLACE(started_at,'T',' ') FROM runs ORDER BY id DESC LIMIT 8;")
        d["runs"] = [(r[0], r[1], r[2]) for r in rows]

    return d


# ── Customer.io campaign metrics ───────────────────────────────────────────────


def _fetch_cio_campaigns(app_key: str) -> list[dict]:
    url = "https://api.customer.io/v1/campaigns?limit=20"
    req = Request(url, headers={"Authorization": f"Bearer {app_key}"})
    try:
        with urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception:
        return []
    campaigns = data.get("campaigns", [])
    campaigns.sort(key=lambda c: c.get("updated", 0), reverse=True)
    return campaigns[:10]


def _fetch_campaign_metrics(cid: int, app_key: str) -> dict:
    url = f"https://api.customer.io/v1/campaigns/{cid}/metrics"
    req = Request(url, headers={"Authorization": f"Bearer {app_key}"})
    try:
        with urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        series = d.get("metric", {}).get("series", {})
        return {k: sum(v) for k, v in series.items()}
    except Exception:
        return {}


def _collect_cio(app_key: str) -> list[dict]:
    campaigns = _fetch_cio_campaigns(app_key)
    if not campaigns:
        return []
    with ThreadPoolExecutor(max_workers=10) as ex:
        metrics_list = list(ex.map(lambda c: _fetch_campaign_metrics(c["id"], app_key), campaigns))
    results = []
    for c, m in zip(campaigns, metrics_list):
        sent = m.get("sent", 0)
        delivered = m.get("delivered", 0)
        clicked = m.get("human_clicked", m.get("clicked", 0))
        converted = m.get("converted", 0)
        unsubs = m.get("unsubscribed", 0)
        pct = f"{converted / delivered * 100:.1f}%" if delivered else "—"
        results.append({
            "name": c.get("name", "—"),
            "sent": sent,
            "delivered": delivered,
            "clicked": clicked,
            "converted": converted,
            "pct": pct,
            "unsubs": unsubs,
        })
    return results


# ── HTML rendering ─────────────────────────────────────────────────────────────

_STYLE = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0f172a; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #e2e8f0; }
.wrap { max-width: 680px; margin: 0 auto; background: #0f172a; }

/* Header */
.hdr { background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%); padding: 28px 32px 20px; border-bottom: 1px solid #1e293b; }
.hdr h1 { font-size: 22px; font-weight: 700; color: #38bdf8; letter-spacing: -0.3px; }
.hdr .sub { font-size: 12px; color: #64748b; margin-top: 4px; }

/* Summary bar */
.summary { display: flex; background: #1e293b; border-bottom: 1px solid #334155; }
.s-cell { flex: 1; padding: 14px 20px; text-align: center; border-right: 1px solid #334155; }
.s-cell:last-child { border-right: none; }
.s-label { font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px; color: #64748b; margin-bottom: 4px; }
.s-val { font-size: 22px; font-weight: 700; color: #f1f5f9; }
.s-val.green { color: #4ade80; }
.s-val.amber { color: #fbbf24; }

/* Section headers */
.sec-hdr { padding: 18px 24px 6px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: #475569; }

/* Source cards — 3-per-row table layout for email-client compat */
.cards-table { width: 100%; border-collapse: collapse; border-top: 1px solid #1e293b; border-bottom: 1px solid #1e293b; }
.cards-table td { vertical-align: top; width: 33.333%; padding: 0; border: 1px solid #1e293b; }
.card { background: #0f172a; padding: 14px 18px; height: 100%; width: 100%; display: block; }
.card-title { font-size: 11px; font-weight: 600; color: #94a3b8; margin-bottom: 10px; white-space: nowrap; }
.card-title span { margin-right: 4px; }

/* Card rows: label left, number right */
.card-row { display: flex; justify-content: space-between; align-items: baseline; padding: 3px 0; border-bottom: 1px solid #1a2740; }
.card-row:last-child { border-bottom: none; }
.card-key { font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #475569; padding-right: 8px; white-space: nowrap; }
.card-num { font-size: 13px; font-weight: 700; color: #e2e8f0; text-align: right; white-space: nowrap; }

/* Table */
.tbl-wrap { padding: 0 0 8px; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { padding: 8px 20px; text-align: left; font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px; color: #475569; border-bottom: 1px solid #1e293b; font-weight: 600; }
td { padding: 8px 20px; border-bottom: 1px solid #0f172a; color: #cbd5e1; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #1e293b; }

/* Staleness dots */
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
.dot-green { background: #4ade80; }
.dot-amber { background: #fbbf24; }
.dot-red   { background: #f87171; }
.days-green { color: #4ade80; font-weight: 600; }
.days-amber { color: #fbbf24; font-weight: 600; }
.days-red   { color: #f87171; font-weight: 600; }

/* Run status badges */
.badge { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 10px; font-weight: 600; text-transform: uppercase; }
.badge-done        { background: #14532d; color: #4ade80; }
.badge-running     { background: #422006; color: #fbbf24; }
.badge-failed      { background: #450a0a; color: #f87171; }
.badge-interrupted { background: #2e1065; color: #c4b5fd; }
.badge-unknown     { background: #1e293b; color: #94a3b8; }

/* Footer */
.ftr { padding: 16px 24px; font-size: 11px; color: #334155; border-top: 1px solid #1e293b; text-align: center; }
"""


def _c(n: int) -> str:
    """Format integer with thousands commas."""
    return f"{n:,}"


def _day_cls(days: int) -> str:
    if days == 0:
        return "days-green"
    if days == 1:
        return "days-amber"
    return "days-red"


def _dot_cls(days: int) -> str:
    if days == 0:
        return "dot-green"
    if days == 1:
        return "dot-amber"
    return "dot-red"


def _badge(status: str) -> str:
    cls_map = {
        "done": "badge-done",
        "running": "badge-running",
        "failed": "badge-failed",
        "interrupted": "badge-interrupted",
    }
    cls = cls_map.get(status.lower(), "badge-unknown")
    return f'<span class="badge {cls}">{status}</span>'


def _source_card(title: str, emoji: str, rows: list[tuple[str, int | str]]) -> str:
    """Render a single source card using a nested table for reliable alignment."""
    row_html = ""
    for label, val in rows:
        if not label:
            continue
        formatted = _c(val) if isinstance(val, int) else val
        row_html += (
            f'<tr>'
            f'<td style="font-size:10px;text-transform:uppercase;letter-spacing:0.5px;color:#475569;padding:4px 0 4px 0;white-space:nowrap;border-bottom:1px solid #1a2740;">{label}</td>'
            f'<td style="font-size:13px;font-weight:700;color:#e2e8f0;text-align:right;padding:4px 0 4px 8px;white-space:nowrap;border-bottom:1px solid #1a2740;">{formatted}</td>'
            f'</tr>'
        )
    return (
        f'<div style="background:#0f172a;padding:14px 18px;width:100%;">'
        f'<div style="font-size:11px;font-weight:600;color:#94a3b8;margin-bottom:8px;white-space:nowrap;">{emoji}&nbsp;{title}</div>'
        f'<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">'
        f'<tbody>{row_html}</tbody>'
        f'</table>'
        f'</div>'
    )


def _build_html(d: dict, cio: list[dict], generated_at: str, db_path: str) -> str:
    # ── Summary bar ──────────────────────────────────────────────────────────
    summary_bar = f"""
  <div class="summary">
    <div class="s-cell">
      <div class="s-label">Total Leads</div>
      <div class="s-val">{_c(d['total_leads'])}</div>
    </div>
    <div class="s-cell">
      <div class="s-label">With Email</div>
      <div class="s-val">{_c(d['p_leads'] + d['r_leads'])}</div>
    </div>
    <div class="s-cell">
      <div class="s-label">In Outbox</div>
      <div class="s-val {'amber' if d['total_unsent'] > 0 else 'green'}">{_c(d['total_unsent'])}</div>
    </div>
    <div class="s-cell">
      <div class="s-label">Sent Today</div>
      <div class="s-val {'green' if d['emitted_today'] > 0 else ''}">{_c(d['emitted_today'])}</div>
    </div>
  </div>"""

    # ── Source cards — 3-per-row via HTML table ──────────────────────────────
    card_devpost = _source_card("Devpost Hackathons", "🏆", [
        ("Hackathons", f"{_c(d['scraped_h'])} / {_c(d['total_h'])}"),
        ("Participants", d["total_p"]),
        ("Emails", d["w_email_p"]),
        ("Outbox", d["unsent_p"]),
    ])
    card_forks = _source_card("GitHub Forks", "🍴", [
        ("Repos", d["total_f_repos"]),
        ("Total", d["total_f"]),
        ("Emails", d["w_email_f"]),
        ("Outbox", d["unsent_f"]),
    ])
    card_search = _source_card("GitHub Search", "🔍", [
        ("Queries", d["total_s_queries"]),
        ("Total", d["total_s"]),
        ("Emails", d["w_email_s"]),
        ("Outbox", d["unsent_s"]),
    ])
    card_hn = _source_card("HN Show", "🟠", [
        ("Posts", d["total_hn"]),
        ("Emails", d["w_email_hn"]),
        ("Outbox", d["unsent_hn"]),
    ])
    if d["devto_exists"] and d["total_dt_challenges"] > 0:
        card_devto = _source_card("dev.to Challenges", "🟣", [
            ("Challenges", f"{_c(d['scraped_dt_challenges'])} / {_c(d['total_dt_challenges'])}"),
            ("Total", d["total_dt"]),
            ("Emails", d["w_email_dt"]),
            ("Outbox", d["unsent_dt"]),
        ])
    else:
        card_devto = _source_card("dev.to Challenges", "🟣", [("Status", "No data yet")])

    if d["rb2b_exists"] and d["total_v"] > 0:
        card_rb2b = _source_card("RB2B Visitors", "👁", [
            ("Visitors", d["total_v"]),
            ("Identified", d["identified_v"]),
            ("Outbox", d["unsent_v"]),
            ("Window", f"{d['visit_first_v']} – {d['visit_last_v']}"),
        ])
    else:
        card_rb2b = _source_card("RB2B Visitors", "👁", [("Status", "No data yet")])

    cards_section = f"""
  <div class="sec-hdr">Data Sources</div>
  <table class="cards-table">
    <tr>
      <td>{card_devpost}</td>
      <td>{card_forks}</td>
      <td>{card_search}</td>
    </tr>
    <tr>
      <td>{card_hn}</td>
      <td>{card_devto}</td>
      <td>{card_rb2b}</td>
    </tr>
  </table>"""

    # ── Staleness table ──────────────────────────────────────────────────────
    sources = [
        ("rb2b", "RB2B",           d["last_v"],  d["last_v_days"]),
        ("devto", "dev.to",        d["last_dt"], d["last_dt_days"]),
        ("hn",   "HN Show",        d["last_hn"], d["last_hn_days"]),
        ("f",    "GitHub Forks",   d["last_f"],  d["last_f_days"]),
        ("s",    "GitHub Search",  d["last_s"],  d["last_s_days"]),
        ("h",    "Devpost",        d["last_h"],  d["last_h_days"]),
    ]
    stale_rows = ""
    for _, name, ts, days in sources:
        dot = f'<span class="dot {_dot_cls(days)}"></span>'
        day_str = f'<span class="{_day_cls(days)}">{days}d</span>' if ts != "never" else '<span class="days-red">—</span>'
        stale_rows += f"<tr><td>{dot}{name}</td><td>{day_str}</td><td>{ts}</td></tr>"

    staleness_section = f"""
  <div class="sec-hdr">Data Freshness</div>
  <div class="tbl-wrap">
    <table>
      <thead><tr><th>Source</th><th>Age</th><th>Last Updated</th></tr></thead>
      <tbody>{stale_rows}</tbody>
    </table>
  </div>"""

    # ── Recent runs ──────────────────────────────────────────────────────────
    if d["runs"]:
        run_rows = ""
        for cmd, status, ts in d["runs"]:
            label = cmd.replace("signalforge-", "")
            time_str = str(ts)[11:16] if ts and len(str(ts)) > 10 else str(ts)
            run_rows += f"<tr><td>{label}</td><td>{_badge(status)}</td><td>{time_str}</td><td style='color:#475569'>{str(ts)[:10]}</td></tr>"
        runs_section = f"""
  <div class="sec-hdr">Recent Runs</div>
  <div class="tbl-wrap">
    <table>
      <thead><tr><th>Command</th><th>Status</th><th>Time</th><th>Date</th></tr></thead>
      <tbody>{run_rows}</tbody>
    </table>
  </div>"""
    else:
        runs_section = ""

    # ── Customer.io campaigns ────────────────────────────────────────────────
    if cio:
        cio_rows = ""
        for c in cio:
            pct_col = f"{c['converted']:,} ({c['pct']})"
            cio_rows += (
                f"<tr>"
                f"<td style='max-width:180px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis'>{c['name']}</td>"
                f"<td style='text-align:right'>{c['sent']:,}</td>"
                f"<td style='text-align:right'>{c['delivered']:,}</td>"
                f"<td style='text-align:right'>{c['clicked']:,}</td>"
                f"<td style='text-align:right'>{pct_col}</td>"
                f"<td style='text-align:right'>{c['unsubs']:,}</td>"
                f"</tr>"
            )
        cio_section = f"""
  <div class="sec-hdr">📬 Customer.io Campaigns</div>
  <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th>Campaign</th>
        <th style="text-align:right">Sent</th>
        <th style="text-align:right">Deliv</th>
        <th style="text-align:right">Clicks</th>
        <th style="text-align:right">Converts</th>
        <th style="text-align:right">Unsubs</th>
      </tr></thead>
      <tbody>{cio_rows}</tbody>
    </table>
  </div>"""
    else:
        cio_section = ""

    # ── Footer ───────────────────────────────────────────────────────────────
    footer = f"""
  <div class="ftr">
    Generated {generated_at} &nbsp;·&nbsp; SignalForge &nbsp;·&nbsp; {db_path}
  </div>"""

    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SignalForge Hourly Report</title>
<style>{_STYLE}</style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <h1>⚡ SignalForge — Hourly Report</h1>
    <div class="sub">Always Be Scraping &nbsp;·&nbsp; {generated_at}</div>
  </div>
{summary_bar}
{cards_section}
{staleness_section}
{runs_section}
{cio_section}
{footer}
</div>
</body>
</html>"""


# ── Plain-text fallback ────────────────────────────────────────────────────────


def _build_plaintext(d: dict, generated_at: str) -> str:
    lines = [
        "SignalForge — Hourly Report",
        f"Generated: {generated_at}",
        "",
        "== Summary ==",
        f"  Total leads:   {_c(d['total_leads'])}",
        f"  With email:    {_c(d['p_leads'] + d['r_leads'])}",
        f"  In outbox:     {_c(d['total_unsent'])}",
        f"  Sent today:    {_c(d['emitted_today'])}",
        "",
        "== Devpost Hackathons ==",
        f"  {_c(d['scraped_h'])}/{_c(d['total_h'])} scraped  |  {_c(d['total_p'])} participants  |  {_c(d['w_email_p'])} emails  |  {_c(d['unsent_p'])} outbox",
        "",
        "== GitHub Forks ==",
        f"  {_c(d['total_f_repos'])} repos  |  {_c(d['total_f'])} total  |  {_c(d['w_email_f'])} emails  |  {_c(d['unsent_f'])} outbox",
        "",
        "== GitHub Search ==",
        f"  {_c(d['total_s_queries'])} queries  |  {_c(d['total_s'])} total  |  {_c(d['w_email_s'])} emails  |  {_c(d['unsent_s'])} outbox",
        "",
        "== HN Show ==",
        f"  {_c(d['total_hn'])} posts  |  {_c(d['w_email_hn'])} emails  |  {_c(d['unsent_hn'])} outbox",
    ]
    if d["devto_exists"]:
        lines += [
            "",
            "== dev.to ==",
            f"  {_c(d['scraped_dt_challenges'])}/{_c(d['total_dt_challenges'])} challenges  |  {_c(d['total_dt'])} total  |  {_c(d['w_email_dt'])} emails  |  {_c(d['unsent_dt'])} outbox",
        ]
    if d["rb2b_exists"]:
        lines += [
            "",
            "== RB2B ==",
            f"  {_c(d['total_v'])} visitors  |  {_c(d['identified_v'])} identified  |  {_c(d['unsent_v'])} outbox",
        ]
    if d["runs"]:
        lines += ["", "== Recent Runs =="]
        for cmd, status, ts in d["runs"]:
            label = cmd.replace("signalforge-", "")
            lines.append(f"  {label:<20}  {status:<12}  {ts}")
    return "\n".join(lines)


# ── CLI entry point ────────────────────────────────────────────────────────────


def report_main() -> None:
    load_dotenv(override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-report",
        description="Build and email an hourly SignalForge dashboard report.",
    )
    parser.add_argument("--to", default="", help="Recipient email (default: REPORT_TO or GMAIL_USER)")
    parser.add_argument("--db", default="", help="Path to SQLite DB (default: devpost_harvest.db)")
    parser.add_argument("--dry-run", action="store_true", help="Print HTML to stdout, do not send")
    args = parser.parse_args()

    # Resolve DB path
    db_path = args.db or os.getenv("HARVEST_DB", "devpost_harvest.db")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.getcwd(), db_path)
    if not os.path.exists(db_path):
        print(f"ERROR: database not found: {db_path}")
        raise SystemExit(1)

    # Resolve recipient
    to_email = args.to or os.getenv("REPORT_TO") or os.getenv("GMAIL_USER", "")
    if not to_email and not args.dry_run:
        print("ERROR: no recipient — set REPORT_TO or pass --to")
        raise SystemExit(1)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"SignalForge Report — {datetime.now().strftime('%a %b %-d, %-I:%M %p')}"

    print("Collecting data from SQLite…")
    d = _collect(db_path)

    # Customer.io campaigns (optional)
    cio_key = os.getenv("CUSTOMERIO_APP_API_KEY", "")
    cio: list[dict] = []
    if cio_key:
        print("Fetching Customer.io campaign metrics…")
        cio = _collect_cio(cio_key)

    html = _build_html(d, cio, generated_at, db_path)
    plain = _build_plaintext(d, generated_at)

    if args.dry_run:
        print(html)
        return

    print(f"Sending report to {to_email}…")
    req = SendEmailRequest(
        to_email=to_email,
        subject=subject,
        body=plain,
        html_body=html,
        from_name="SignalForge",
    )
    result = send_email(req)
    if result.success:
        print(f"✓ Report sent to {to_email}")
    else:
        print(f"✗ Failed: {result.error}")
        raise SystemExit(1)
