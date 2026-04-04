"""signalforge-assistant — interactive AI analyst REPL for the lead database."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import argcomplete
from dotenv import load_dotenv, set_key

from devpost_scraper.backboard_client import BackboardClientError, build_client, ensure_assistant
from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _PARTICIPANTS_JWT_KEY,
    _SF_ASSISTANT_ID_KEY,
    _RESET, _BOLD, _DIM, _CYAN, _YELLOW, _GREEN, _RED,
)
from devpost_scraper.cli_github_forks import _run_github_forks
from devpost_scraper.cli_harvest import _run_harvest
from devpost_scraper.cli_rb2b import _run_rb2b


_SF_SYSTEM_PROMPT = """\
You are SignalForge Assistant — an expert data analyst for the SignalForge lead generation platform.

You have access to a SQLite database with three core tables:

HACKATHONS — Devpost hackathon events
  Columns: id, url, title, organization_name, open_state, submission_period_dates,
           registrations_count, prize_amount, themes, invite_only, first_seen_at, last_scraped_at

PARTICIPANTS — All tracked leads (hackathon participants, GitHub fork owners, GitHub search hits)
  Columns: hackathon_url, hackathon_title, username, name, specialty, profile_url,
           github_url, linkedin_url, email, first_seen_at, last_seen_at, event_emitted_at
  Note: hackathon_url prefixes —
        'github:forks:<repo>'  → GitHub fork targets
        'github:search:<query>' → GitHub search results
        anything else          → Devpost hackathon participants

RB2B_VISITORS — Website visitors identified via RB2B
  Columns: visitor_id, email, first_name, last_name, linkedin_url, company_name, title,
           industry, employee_count, estimated_revenue, city, state, website,
           rb2b_last_seen_at, rb2b_first_seen_at, most_recent_referrer, recent_page_urls,
           profile_type, imported_at, event_emitted_at

Available tools:
- get_db_schema  → inspect tables and columns
- query_db(sql)  → run any SELECT/WITH query, returns rows as JSON
- export_csv(sql, filename) → run a SELECT and save results to disk
- web search (built-in, automatic) → searches the live web for anything outside the
  database: company info, LinkedIn lookups, hackathon context, current events, enrichment.
  Use it freely when the user asks about something you can't answer from the DB alone.

Scrape tools (safe — no Customer.io events emitted, DB writes only):
- scrape_github_forks(repo, max_forks) → mine fork owners for a GitHub repo (email enrichment always on)
  repo format: "owner/repo"  |  max_forks default 200
- scrape_rb2b_visitors(date) → fetch & import an RB2B website-visitor export
  date format: "YYYY-MM-DD"  |  defaults to today
- scrape_devpost_harvest(pages, statuses, max_participants) → crawl Devpost
  hackathon listings + participants (email enrichment always on)
  pages default 1  |  statuses default ["open"]  |  max_participants default 50

Critical schema conventions (ALWAYS follow these):
- Missing/unknown values are stored as empty string '' — NOT as NULL.
  CORRECT:   WHERE email != '' AND email IS NOT NULL
  CORRECT:   WHERE email = ''
  WRONG:     WHERE email IS NULL          ← returns 0, all missing emails are ''
  WRONG:     WHERE email IS NOT NULL      ← returns everyone, even those with no email
  This applies to email, github_url, linkedin_url, and all text fields.
- event_emitted_at IS NULL means event not yet emitted (this column uses real NULLs).
- LIKE searches: hackathon titles often contain apostrophes (e.g. "World's Largest Hackathon").
  Use tokenized wildcard patterns so they match regardless of punctuation:
  CORRECT: WHERE title LIKE '%World%Largest%'
  WRONG:   WHERE title LIKE '%Worlds Largest%'   ← misses the apostrophe in "World's"
  WRONG:   WHERE title LIKE "%World's Largest%"  ← breaks on single-quote escaping
  Rule: split the user's words into separate % tokens joined by AND or chained in one pattern.

Guidelines:
- Always LIMIT large queries unless the user explicitly asks for all rows.
- Show counts and percentages when answering coverage/funnel questions.
- For email coverage: COUNT(CASE WHEN email != '' AND email IS NOT NULL THEN 1 END) / COUNT(*) * 100
- When exporting, confirm the filename and row count.
- Be concise and data-driven. Offer follow-up suggestions when relevant.
"""

_SF_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_db_schema",
            "description": "Get the full SQLite database schema — all tables, columns, and types.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_db",
            "description": (
                "Run a readonly SELECT or WITH query on the SQLite database. "
                "Returns results as a JSON array of objects."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A valid SELECT or WITH SQL query. Include LIMIT unless fetching all rows.",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "export_csv",
            "description": "Run a SELECT query and export all matching rows to a CSV file on disk.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "A valid SELECT or WITH SQL query."},
                    "filename": {"type": "string", "description": "Output CSV filename, e.g. 'leads_with_email.csv'."},
                },
                "required": ["sql", "filename"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scrape_github_forks",
            "description": (
                "Scrape fork owners for a GitHub repo and upsert them into the DB. "
                "No Customer.io events are emitted. Progress is printed to the terminal."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "repo": {"type": "string", "description": "GitHub repo in 'owner/repo' format, e.g. 'backboard-io/backboard'."},
                    "max_forks": {"type": "integer", "description": "Max forks to process (default 200)."},
                },
                "required": ["repo"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scrape_rb2b_visitors",
            "description": (
                "Fetch today's (or a past date's) RB2B website-visitor export and import "
                "new visitors into the DB. No Customer.io events are emitted."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "Export date in YYYY-MM-DD format (default: today)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scrape_devpost_harvest",
            "description": (
                "Crawl Devpost hackathon listings and scrape participants into the DB. "
                "No Customer.io events are emitted."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "pages": {"type": "integer", "description": "Hackathon listing pages to fetch (default 1, ~9 hackathons/page)."},
                    "statuses": {"type": "array", "items": {"type": "string"}, "description": "Hackathon statuses to include: 'open', 'ended', 'upcoming' (default ['open'])."},
                    "max_participants": {"type": "integer", "description": "Max participants to scrape per hackathon (default 50)."},
                },
                "required": [],
            },
        },
    },
]


# ─── Tool handlers ────────────────────────────────────────────────────────────

async def _sf_get_db_schema(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    import sqlite3 as _sq3
    conn = _sq3.connect(db_path)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    schema: dict[str, Any] = {}
    for (tname,) in tables:
        cols = conn.execute(f"PRAGMA table_info({tname})").fetchall()
        schema[tname] = [
            {"name": c[1], "type": c[2], "not_null": bool(c[3]), "pk": bool(c[5])}
            for c in cols
        ]
    conn.close()
    return {"schema": schema}


async def _sf_query_db(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    import sqlite3 as _sq3
    sql = args.get("sql", "").strip()
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        return {"error": "Only SELECT/WITH queries are allowed (readonly mode)."}
    try:
        conn = _sq3.connect(db_path)
        conn.row_factory = _sq3.Row
        rows = conn.execute(sql).fetchall()
        conn.close()
        return {"rows": [dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        return {"error": str(exc)}


async def _sf_export_csv(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    import csv
    import sqlite3 as _sq3
    sql = args.get("sql", "").strip()
    filename = args.get("filename", "export.csv")
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        return {"error": "Only SELECT/WITH queries are allowed."}
    if not filename.endswith(".csv"):
        filename += ".csv"
    try:
        conn = _sq3.connect(db_path)
        conn.row_factory = _sq3.Row
        rows = conn.execute(sql).fetchall()
        conn.close()
        if not rows:
            return {"error": "Query returned no rows.", "filename": filename}
        out = Path(filename)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])
        return {"ok": True, "filename": str(out.resolve()), "rows_exported": len(rows)}
    except Exception as exc:
        return {"error": str(exc)}


async def _sf_scrape_github_forks(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    from devpost_scraper.db import HarvestDB
    repo = args.get("repo", "").strip()
    if "/" not in repo:
        return {"error": "repo must be in 'owner/repo' format, e.g. 'microsoft/vscode'"}
    owner, repo_name = repo.split("/", 1)
    max_forks = int(args.get("max_forks") or 200)

    db = HarvestDB(db_path)
    before = db.stats()
    db.close()

    print(f"\n  {_DIM}Scraping forks for {owner}/{repo_name} (max={max_forks})…{_RESET}", flush=True)
    try:
        await _run_github_forks(
            owner=owner,
            repo=repo_name,
            max_forks=max_forks,
            fork_mode="top_by_pushed",
            db_path=db_path,
            no_email=False,
            emit_events=False,
            force_email=False,
        )
    except SystemExit as exc:
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": f"Scrape failed: {exc}"}

    db2 = HarvestDB(db_path)
    after = db2.stats()
    db2.close()
    return {
        "ok": True,
        "repo": f"{owner}/{repo_name}",
        "new_fork_owners": after["participants"] - before["participants"],
        "new_with_email": after["with_email"] - before["with_email"],
        "total_participants": after["participants"],
    }


async def _sf_scrape_rb2b(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    from datetime import date as _date
    from devpost_scraper.db import HarvestDB
    fetch_date = (args.get("date") or _date.today().isoformat()).strip()
    rb2b_session = os.getenv("RB2B_SESSION", "").strip()
    reb2b_uid = os.getenv("REB2B_UID", "").strip()

    if not rb2b_session or not reb2b_uid:
        return {
            "error": (
                "RB2B_SESSION and REB2B_UID must be set in .env. "
                "Copy _rb2b_session and _reb2buid from browser DevTools → "
                "Application → Cookies → app.rb2b.com"
            )
        }

    db = HarvestDB(db_path)
    before = db.rb2b_stats()
    db.close()

    print(f"\n  {_DIM}Fetching RB2B export for {fetch_date}…{_RESET}", flush=True)
    try:
        await _run_rb2b(
            csv_paths=[],
            db_path=db_path,
            emit_events=False,
            emit_unsent=False,
            fetch_date=fetch_date,
            rb2b_session=rb2b_session,
            reb2b_uid=reb2b_uid,
        )
    except SystemExit as exc:
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": f"RB2B fetch failed: {exc}"}

    db2 = HarvestDB(db_path)
    after = db2.rb2b_stats()
    db2.close()
    return {
        "ok": True,
        "date": fetch_date,
        "new_visitors": after["total"] - before["total"],
        "new_identified": after["identified"] - before["identified"],
        "total_visitors": after["total"],
        "total_identified": after["identified"],
    }


async def _sf_scrape_harvest(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    from devpost_scraper.db import HarvestDB
    pages = int(args.get("pages") or 1)
    statuses: list[str] = args.get("statuses") or ["open"]
    max_participants = int(args.get("max_participants") or 50)

    jwt_token = os.getenv(_PARTICIPANTS_JWT_KEY, "").strip()
    if not jwt_token:
        return {
            "error": (
                "DEVPOST_SESSION not set in .env. "
                "Copy the _devpost cookie from browser DevTools → Application → Cookies."
            )
        }

    db = HarvestDB(db_path)
    before = db.stats()
    db.close()

    print(
        f"\n  {_DIM}Harvesting Devpost: {pages} page(s), statuses={statuses}, "
        f"max_participants={max_participants}…{_RESET}",
        flush=True,
    )
    try:
        await _run_harvest(
            pages=pages,
            jwt_token=jwt_token,
            db_path=db_path,
            no_email=False,
            emit_events=False,
            rescrape=False,
            max_participants=max_participants,
            statuses=statuses,
        )
    except SystemExit as exc:
        return {"error": str(exc)}
    except Exception as exc:
        return {"error": f"Harvest failed: {exc}"}

    db2 = HarvestDB(db_path)
    after = db2.stats()
    db2.close()
    return {
        "ok": True,
        "pages": pages,
        "statuses": statuses,
        "new_hackathons": after["hackathons"] - before["hackathons"],
        "new_participants": after["participants"] - before["participants"],
        "new_with_email": after["with_email"] - before["with_email"],
        "total_hackathons": after["hackathons"],
        "total_participants": after["participants"],
    }


_SF_TOOL_HANDLERS: dict[str, Any] = {
    "get_db_schema": _sf_get_db_schema,
    "query_db": _sf_query_db,
    "export_csv": _sf_export_csv,
    "scrape_github_forks": _sf_scrape_github_forks,
    "scrape_rb2b_visitors": _sf_scrape_rb2b,
    "scrape_devpost_harvest": _sf_scrape_harvest,
}

# Tools that run long scraping jobs — skip the spinner and let progress print naturally
_SF_SLOW_TOOLS = {"scrape_github_forks", "scrape_rb2b_visitors", "scrape_devpost_harvest"}


# ─── Spinner ─────────────────────────────────────────────────────────────────

async def _sf_spinner(message: str) -> None:
    frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    i = 0
    try:
        while True:
            print(f"\r  {_DIM}{frames[i % len(frames)]}  {message}{_RESET}   ", end="", flush=True)
            i += 1
            await asyncio.sleep(0.08)
    except asyncio.CancelledError:
        print(f"\r{' ' * 40}\r", end="", flush=True)


# ─── Streaming drain with live output ────────────────────────────────────────

def _sf_render_markdown(text: str) -> None:
    """Render markdown text to the terminal using rich."""
    from rich.console import Console
    from rich.markdown import Markdown
    console = Console(highlight=False)
    console.print(Markdown(text), end="")


async def _sf_drain_stream(
    client: Any,
    thread_id: str,
    stream: Any,
    db_path: str,
    depth: int = 0,
) -> None:
    """Drain one streaming turn: buffer content, render markdown, execute tool calls."""
    tool_calls: list[Any] = []
    run_id: str | None = None
    had_error = False
    seen_chunks: list[str] = []
    content_parts: list[str] = []

    async for chunk in stream:
        t = chunk.get("type")
        seen_chunks.append(t or "?")
        if t == "content_streaming":
            token = chunk.get("content", "")
            content_parts.append(token)
            total = sum(len(p) for p in content_parts)
            print(f"\r  {_DIM}receiving… {total:,} chars{_RESET}   ", end="", flush=True)
        elif t == "tool_submit_required":
            run_id = chunk.get("run_id")
            tool_calls = chunk.get("tool_calls", [])
        elif t in ("error", "run_failed"):
            had_error = True
            msg = chunk.get("error") or chunk.get("message") or "unknown error"
            print(f"\r{_RED}✗ {msg}{_RESET}                    ", flush=True)
            return
        elif t == "run_ended":
            status = chunk.get("status")
            if status not in (None, "completed"):
                had_error = True
                print(f"\r{_RED}✗ run ended with status: {status}{_RESET}   ", flush=True)
                return

    full_content = "".join(content_parts)

    if not tool_calls or not run_id:
        if full_content:
            print(f"\r{' ' * 40}\r", end="", flush=True)
            _sf_render_markdown(full_content)
        elif not had_error:
            event_summary = " → ".join(seen_chunks) if seen_chunks else "no events"
            print(
                f"\r{_YELLOW}⚠  Empty response ({event_summary}). "
                f"Try /reset to start a fresh thread.{_RESET}   "
            )
        return

    print()

    tool_outputs = []
    for tc in tool_calls:
        name = tc["function"]["name"] if isinstance(tc, dict) else tc.function.name
        args_raw = (
            tc["function"].get("arguments", "{}") if isinstance(tc, dict)
            else (tc.function.arguments or "{}")
        )
        args = args_raw if isinstance(args_raw, dict) else json.loads(args_raw or "{}")
        tc_id = tc["id"] if isinstance(tc, dict) else tc.id

        arg_preview = ", ".join(f"{k}={repr(v)[:60]}" for k, v in args.items())
        print(f"  {_YELLOW}⚙  {name}({arg_preview}){_RESET}", flush=True)

        handler = _SF_TOOL_HANDLERS.get(name)
        if handler is None:
            result: dict[str, Any] = {"error": f"Unknown tool: {name}"}
        elif name in _SF_SLOW_TOOLS:
            result = await handler(args, db_path)
        else:
            spinner_task = asyncio.create_task(_sf_spinner("working…"))
            try:
                result = await handler(args, db_path)
            finally:
                spinner_task.cancel()
                try:
                    await spinner_task
                except asyncio.CancelledError:
                    pass

        if "error" in result:
            print(f"  {_RED}✗ {result['error']}{_RESET}", flush=True)
        elif "rows" in result:
            print(f"  {_GREEN}✓ {result['count']:,} row(s) returned{_RESET}", flush=True)
        elif result.get("ok") and "rows_exported" in result:
            print(f"  {_GREEN}✓ {result['rows_exported']:,} rows → {result['filename']}{_RESET}", flush=True)
        elif result.get("ok") and name == "scrape_github_forks":
            print(
                f"  {_GREEN}✓ {result['new_fork_owners']:,} new fork owners "
                f"(+{result['new_with_email']:,} emails) — "
                f"{result['total_participants']:,} total{_RESET}",
                flush=True,
            )
        elif result.get("ok") and name == "scrape_rb2b_visitors":
            print(
                f"  {_GREEN}✓ +{result['new_visitors']:,} visitors "
                f"(+{result['new_identified']:,} identified) — "
                f"{result['total_visitors']:,} total{_RESET}",
                flush=True,
            )
        elif result.get("ok") and name == "scrape_devpost_harvest":
            print(
                f"  {_GREEN}✓ +{result['new_hackathons']:,} hackathons, "
                f"+{result['new_participants']:,} participants "
                f"(+{result['new_with_email']:,} emails){_RESET}",
                flush=True,
            )
        else:
            print(f"  {_GREEN}✓ done{_RESET}", flush=True)

        tool_outputs.append({"tool_call_id": tc_id, "output": json.dumps(result)})

    print(f"\n{_BOLD}SignalForge{_RESET}   ", end="", flush=True)
    stream2 = await client.submit_tool_outputs(
        thread_id=thread_id,
        run_id=run_id,
        tool_outputs=tool_outputs,
        stream=True,
    )
    await _sf_drain_stream(client, thread_id, stream2, db_path, depth + 1)


# ─── REPL ─────────────────────────────────────────────────────────────────────

_SF_WELCOME = f"""\
{_BOLD}{_CYAN}
  ╔══════════════════════════════════════════════════╗
  ║   SignalForge Assistant                          ║
  ║   AI analyst for your lead database             ║
  ╚══════════════════════════════════════════════════╝
{_RESET}"""

_SF_HELP = f"""
{_BOLD}Slash commands:{_RESET}
  /help         Show this message
  /exit         Quit the session
  /reset        Start a fresh conversation thread (fixes stuck/empty responses)
  /stats        DB statistics summary
  /schema       Show database schema
  /clear        Clear terminal
  /db <path>    Switch to a different SQLite file

{_BOLD}Example questions:{_RESET}
  "How many participants have email addresses?"
  "Top 10 hackathons by participant count"
  "Export all RB2B visitors with LinkedIn URLs to rb2b_linkedin.csv"
  "Show participants from AI-related hackathons with email, limit 20"
  "What % of GitHub fork targets do we have emails for?"
"""


async def _run_assistant_repl(db_path: str, llm_provider: str, model_name: str) -> None:
    load_dotenv(_ENV_FILE, override=True)

    print(_SF_WELCOME)

    db_file = Path(db_path)
    if db_file.exists():
        import sqlite3 as _sq3
        _c2 = _sq3.connect(db_path)
        hcount = _c2.execute("SELECT COUNT(*) FROM hackathons").fetchone()[0]
        pcount = _c2.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
        with_email = _c2.execute(
            "SELECT COUNT(*) FROM participants WHERE email != '' AND email IS NOT NULL"
        ).fetchone()[0]
        rb2b = _c2.execute("SELECT COUNT(*) FROM rb2b_visitors").fetchone()[0]
        _c2.close()
        pct = f"{with_email / pcount * 100:.0f}%" if pcount else "N/A"
        print(f"  {_DIM}DB: {db_path}{_RESET}")
        print(
            f"  {_DIM}{hcount:,} hackathons · "
            f"{pcount:,} participants ({pct} with email) · "
            f"{rb2b:,} RB2B visitors{_RESET}"
        )
    else:
        print(f"  {_YELLOW}⚠  DB not found at {db_path!r} — queries will fail{_RESET}")

    print(f"\n  {_DIM}Type a question, /help for commands, or /exit to quit{_RESET}\n")

    client = build_client()

    stored_id = os.getenv(_SF_ASSISTANT_ID_KEY, "").strip()
    if stored_id:
        print(f"  {_DIM}Reusing assistant {stored_id}{_RESET}")
        assistant_id = stored_id
    else:
        print(f"  {_DIM}Creating SignalForge assistant…{_RESET}", end="", flush=True)
        aid = await ensure_assistant(
            client,
            assistant_id=None,
            name="signalforge-assistant-v1",
            system_prompt=_SF_SYSTEM_PROMPT,
            tools=_SF_TOOLS,
        )
        assistant_id = str(aid)
        _ENV_FILE.touch(exist_ok=True)
        set_key(str(_ENV_FILE), _SF_ASSISTANT_ID_KEY, assistant_id)
        print(f" {_GREEN}✓{_RESET} {_DIM}saved to .env ({assistant_id}){_RESET}")

    async def _new_thread() -> str:
        t = await client.create_thread(assistant_id)
        return t.thread_id

    thread_id = await _new_thread()
    print(f"  {_DIM}Session thread: {thread_id}{_RESET}\n")

    while True:
        try:
            user_input = input(f"{_BOLD}You{_RESET}  ").strip()
        except (KeyboardInterrupt, EOFError):
            print(f"\n\n  {_DIM}Goodbye!{_RESET}\n")
            break

        if not user_input:
            continue

        if user_input.startswith("/"):
            cmd = user_input.lower().split()[0]
            if cmd == "/exit":
                print(f"\n  {_DIM}Goodbye!{_RESET}\n")
                break
            elif cmd == "/reset":
                thread_id = await _new_thread()
                print(f"  {_GREEN}✓ Fresh thread started{_RESET} {_DIM}({thread_id}){_RESET}\n")
                continue
            elif cmd == "/help":
                print(_SF_HELP)
                continue
            elif cmd == "/stats":
                if db_file.exists():
                    import sqlite3 as _sq3
                    _c3 = _sq3.connect(db_path)
                    hc = _c3.execute("SELECT COUNT(*) FROM hackathons").fetchone()[0]
                    pc = _c3.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
                    we = _c3.execute(
                        "SELECT COUNT(*) FROM participants WHERE email != '' AND email IS NOT NULL"
                    ).fetchone()[0]
                    em = _c3.execute(
                        "SELECT COUNT(*) FROM participants WHERE event_emitted_at IS NOT NULL"
                    ).fetchone()[0]
                    rb = _c3.execute("SELECT COUNT(*) FROM rb2b_visitors").fetchone()[0]
                    _c3.close()
                    ep = f"{we / pc * 100:.1f}%" if pc else "N/A"
                    print(f"""
  {_BOLD}Database:{_RESET} {db_path}
  Hackathons:      {hc:,}
  Participants:    {pc:,}  ({ep} have email)
  Events emitted:  {em:,}
  RB2B visitors:   {rb:,}
""")
                else:
                    print(f"  {_RED}DB not found: {db_path}{_RESET}")
                continue
            elif cmd == "/schema":
                user_input = "Show me the complete database schema with all tables and columns."
            elif cmd == "/clear":
                print("\033[H\033[2J", end="")
                continue
            elif cmd == "/db":
                parts = user_input.split(maxsplit=1)
                if len(parts) > 1:
                    db_path = parts[1].strip()
                    db_file = Path(db_path)
                    status = _GREEN + "✓ exists" if db_file.exists() else _YELLOW + "⚠ not found"
                    print(f"  {status}{_RESET} {_DIM}→ {db_path}{_RESET}")
                else:
                    print(f"  {_RED}Usage: /db <path>{_RESET}")
                continue
            else:
                print(f"  {_YELLOW}Unknown command. Try /help{_RESET}")
                continue

        print(f"\n{_BOLD}SignalForge{_RESET}   ", end="", flush=True)
        try:
            stream = await client.add_message(
                thread_id=thread_id,
                content=user_input,
                stream=True,
                web_search="Auto",
                llm_provider=llm_provider,
                model_name=model_name,
            )
            await _sf_drain_stream(client, thread_id, stream, db_path)
        except BackboardClientError as exc:
            print(f"\r  {_RED}✗ {exc}{_RESET}                    ")
        except Exception as exc:
            print(f"\r  {_RED}✗ Unexpected error: {exc}{_RESET}                    ")

        print()


def assistant_main() -> None:
    parser = argparse.ArgumentParser(
        prog="signalforge-assistant",
        description="Interactive AI analyst for the SignalForge lead database",
    )
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    load_dotenv(_ENV_FILE, override=True)
    try:
        asyncio.run(
            _run_assistant_repl(
                db_path=args.db,
                llm_provider=os.getenv("BACKBOARD_LLM_PROVIDER", "openai"),
                model_name=os.getenv("BACKBOARD_MODEL", "gpt-4o-mini"),
            )
        )
    except KeyboardInterrupt:
        print(f"\n\n  {_DIM}Goodbye!{_RESET}\n")
