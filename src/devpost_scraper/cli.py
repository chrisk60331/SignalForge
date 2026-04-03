from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_FORK_EMAIL_CONCURRENCY = 1  # concurrent GitHub API calls during fork email enrichment

from dotenv import load_dotenv, set_key

from devpost_scraper.backboard_client import (
    BackboardClientError,
    build_client,
    ensure_assistant,
    run_in_thread,
)
from devpost_scraper.customerio import emit_devto_events, emit_github_fork_events, emit_github_search_events, emit_hackathon_events, emit_visited_site_events, select_event_name
from devpost_scraper.csv_export import write_projects
from devpost_scraper.models import DevpostProject, Hackathon, HackathonParticipant, Rb2bVisitor
from devpost_scraper.scraper import (
    download_rb2b_export,
    fetch_rb2b_exports,
    fetch_repo_forks,
    find_author_email,
    find_participant_email,
    get_devto_challenge_tag,
    get_devto_tag_articles,
    get_github_email,
    get_hackathon_participants,
    get_project_details,
    list_devto_challenges,
    list_hackathons,
    search_github_repos,
    search_projects,
)

_ENV_FILE = Path(".env")
_ASSISTANT_ID_KEY = "DEVPOST_ASSISTANT_ID"
_LANDING_BANNER = r"""
   _____ _                   ________                    
  / ___/(_)___ _____  ____ _/ / ____/___  _________ ____ 
  \__ \/ / __ `/ __ \/ __ `/ / /_  / __ \/ ___/ __ `/ _ \
 ___/ / / /_/ / / / / /_/ / / __/ / /_/ / /  / /_/ /  __/
/____/_/\__, /_/ /_/\__,_/_/_/    \____/_/   \__, /\___/ 
       /____/                               /____/       
"""
_LANDING_MENU = """\
Command Menu:

  [1]  signalforge-devpost-search  → Search Devpost projects + enrich + export CSV
  [2]  signalforge-participants    → Scrape one hackathon's participants + export CSV
  [3]  signalforge-harvest         → Walk hackathons → scrape → SQLite → emit events
  [4]  signalforge-github-forks    → Mine GitHub fork owners + optional email enrichment
  [5]  signalforge-gh-search       → Search GitHub repos by keyword + mine owner emails
  [6]  signalforge-rb2b            → Import RB2B visitor CSVs + emit visited_site events
  [7]  signalforge-auto            → Full daily scrape: RB2B + Harvest + all Forks (no emit)
  [8]  signalforge-auto-batch      → Daily scrape + emit batch in one cron command
  [9]  signalforge-emit-all        → Flush all unsent events across every source at once
  [10] signalforge-emit-batch      → Emit up to --batch-size events per source (cron-friendly)
  [11] signalforge-campaigns       → Sync email HTML with Customer.io campaign actions
   Subcommands: list-campaigns · get-campaign · show-campaign get-actions · update-all · get · update
  [12] signalforge-lookup          → Search the DB by email, name, or username
  [13] signalforge-assistant       → Interactive AI analyst: query DB, export CSV, insights
  [14] signalforge-devto           → Walk dev.to challenges → scrape submitters → SQLite → emit events
"""

# ─── ANSI terminal helpers ──────────────────────────────────────────────────
_ANSI = sys.stdout.isatty()


def _c(code: str) -> str:
    return code if _ANSI else ""


_RESET   = _c("\033[0m")
_BOLD    = _c("\033[1m")
_DIM     = _c("\033[2m")
_CYAN    = _c("\033[96m")
_YELLOW  = _c("\033[93m")
_GREEN   = _c("\033[92m")
_RED     = _c("\033[91m")
_MAGENTA = _c("\033[95m")


def _print_landing() -> None:
    print(_LANDING_BANNER.strip("\n"))
    print()
    print(_LANDING_MENU)


def landing_main() -> None:
    _print_landing()

# The assistant's ONLY job is to search and return raw project URLs.
# Python handles all enrichment directly — no tool loop explosion.
_SYSTEM_PROMPT = """\
You are a Devpost search assistant. Given a search term:

1. Call search_devpost_projects for page 1 and page 2.
2. Deduplicate results by URL.
3. Return ONLY a valid JSON array — no prose, no markdown, no code fences.

Each element: {"title": "...", "tagline": "...", "url": "...", "built_with": "..."}
built_with is a comma-separated string of technology names.
Never call the same tool with the same arguments twice.\
"""

_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "search_devpost_projects",
            "description": "Search Devpost for hackathon projects matching a query.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query term"},
                    "page": {"type": "integer", "description": "Page number (default 1)"},
                },
                "required": ["query"],
            },
        },
    },
]


async def _handle_search(args: dict[str, Any]) -> dict[str, Any]:
    query = args["query"]
    page = int(args.get("page") or 1)
    print(f"  [tool] search_devpost_projects(query={query!r}, page={page})", file=sys.stderr)
    return await search_projects(query=query, page=page)


_TOOL_HANDLERS = {
    "search_devpost_projects": _handle_search,
}


async def _load_or_create_assistant(client: Any) -> str:
    load_dotenv(_ENV_FILE, override=True)
    stored_id = os.getenv(_ASSISTANT_ID_KEY, "").strip()
    if stored_id:
        print(f"[info] Reusing assistant {stored_id}", file=sys.stderr)
        return stored_id

    print("[info] Creating Backboard assistant…", file=sys.stderr)
    aid = await ensure_assistant(
        client,
        assistant_id=None,
        name="devpost-scraper-v3",
        system_prompt=_SYSTEM_PROMPT,
        tools=_TOOLS,
    )
    _ENV_FILE.touch(exist_ok=True)
    set_key(str(_ENV_FILE), _ASSISTANT_ID_KEY, str(aid))
    print(f"[info] Created assistant {aid} — saved to .env", file=sys.stderr)
    return str(aid)


def _parse_search_results(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(
            line for line in raw.splitlines()
            if not line.strip().startswith("```")
        ).strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"[error] Assistant returned invalid JSON: {exc}\n\nRaw:\n{raw}"
        ) from exc
    if not isinstance(data, list):
        raise SystemExit(f"[error] Expected JSON array, got {type(data).__name__}")
    return [item for item in data if isinstance(item, dict) and item.get("url")]


async def _enrich_project(
    item: dict[str, Any],
    search_term: str,
) -> DevpostProject:
    url = item["url"]

    # detail page enrichment
    details: dict[str, Any] = {}
    try:
        details = await get_project_details(url=url)
        print(f"  [enrich] details {url}", file=sys.stderr)
    except Exception as exc:
        print(f"  [warn] details failed for {url}: {exc}", file=sys.stderr)

    # email chain
    email_data: dict[str, Any] = {}
    try:
        email_data = await find_author_email(project_url=url)
        if email_data.get("email"):
            print(f"  [email] {email_data['email']} ← {url}", file=sys.stderr)
        else:
            print(f"  [email] (none found) ← {url}", file=sys.stderr)
    except Exception as exc:
        print(f"  [warn] email failed for {url}: {exc}", file=sys.stderr)

    author_urls: list[str] = email_data.get("author_profile_urls", [])

    return DevpostProject(
        search_term=search_term,
        title=details.get("title") or item.get("title", ""),
        tagline=details.get("tagline") or item.get("tagline", ""),
        url=url,
        hackathon_name=details.get("hackathon_name", ""),
        hackathon_url=details.get("hackathon_url", ""),
        summary=details.get("summary", ""),
        built_with=details.get("built_with") or item.get("built_with", ""),
        prizes=details.get("prizes", ""),
        team_size=details.get("team_size", ""),
        author_profile_url=author_urls[0] if author_urls else "",
        email=email_data.get("email", ""),
    )


async def run(search_terms: list[str], output: str | None) -> None:
    load_dotenv(_ENV_FILE, override=True)
    client = build_client()
    assistant_id = await _load_or_create_assistant(client)

    all_projects: list[DevpostProject] = []

    for term in search_terms:
        print(f"\n[info] Searching Devpost for: {term!r}", file=sys.stderr)
        raw = await run_in_thread(
            client,
            assistant_id=assistant_id,
            user_message=(
                f"Search Devpost for: {term!r}\n"
                "Collect page 1 and page 2. Return a JSON array of projects."
            ),
            tool_handlers=_TOOL_HANDLERS,
            llm_provider=os.getenv("BACKBOARD_LLM_PROVIDER", "openai"),
            model_name=os.getenv("BACKBOARD_MODEL", "gpt-4o-mini"),
        )
        items = _parse_search_results(raw)
        print(f"[info] Found {len(items)} projects — enriching…", file=sys.stderr)

        # Enrich sequentially to be polite to external sites
        projects: list[DevpostProject] = []
        for item in items:
            project = await _enrich_project(item, search_term=term)
            projects.append(project)

        print(f"[info] Collected {len(projects)} projects for {term!r}", file=sys.stderr)
        all_projects.extend(projects)

    print(f"\n[info] Total projects: {len(all_projects)}", file=sys.stderr)
    write_projects(all_projects, output)
    if output:
        print(f"[info] Wrote → {output}", file=sys.stderr)


def main() -> None:
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-devpost-search --help` for full usage.")
    return
    parser = argparse.ArgumentParser(
        prog="signalforge-devpost-search",
        description="Extract Devpost project data and export to CSV.",
    )
    parser.add_argument(
        "search_terms",
        nargs="+",
        metavar="TERM",
        help="One or more search terms to query on Devpost",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        default=None,
        help="Output CSV file path (default: stdout)",
    )
    args = parser.parse_args()
    asyncio.run(run(search_terms=args.search_terms, output=args.output))


if __name__ == "__main__":
    main()


_PARTICIPANTS_JWT_KEY = "DEVPOST_SESSION"
_DEVTO_SESSION_KEY = "DEV_TO__DEVTO_FOREM_SESSION"
_DEVTO_REMEMBER_KEY = "DEV_TO_REMEMBER_USER_TOKEN"
_DEVTO_CURRENT_USER_KEY = "DEV_TO_CURRENT_USER"


async def _run_participants(
    hackathon_url: str,
    jwt_token: str,
    output: str | None,
    no_email: bool,
    emit_events: bool = False,
) -> None:
    all_participants: list[HackathonParticipant] = []
    page = 1

    print(f"[info] Fetching participants from {hackathon_url}", file=sys.stderr)

    while True:
        data = await get_hackathon_participants(hackathon_url, jwt_token, page=page)
        batch = data.get("participants", [])
        has_more = data.get("has_more", False)

        if not batch:
            print(f"[info] No participants on page {page}, stopping.", file=sys.stderr)
            break

        print(f"[info] Page {page}: {len(batch)} participants", file=sys.stderr)

        for raw in batch:
            profile_url = raw.get("profile_url", "")
            email = ""
            github_url = ""
            linkedin_url = ""

            if not no_email and profile_url:
                try:
                    email_data = await find_participant_email(profile_url)
                    email = email_data.get("email", "")
                    github_url = email_data.get("github_url", "")
                    linkedin_url = email_data.get("linkedin_url", "")
                    parts = [f for f in [email, github_url, linkedin_url] if f]
                    if parts:
                        print(f"  [found] {', '.join(parts)} ← {profile_url}", file=sys.stderr)
                    else:
                        print(f"  [none] ← {profile_url}", file=sys.stderr)
                except Exception as exc:
                    print(f"  [warn] enrich failed for {profile_url}: {exc}", file=sys.stderr)

            all_participants.append(
                HackathonParticipant(
                    hackathon_url=hackathon_url,
                    username=raw.get("username", ""),
                    name=raw.get("name", ""),
                    specialty=raw.get("specialty", ""),
                    profile_url=profile_url,
                    github_url=github_url,
                    linkedin_url=linkedin_url,
                    email=email,
                )
            )

        if not has_more:
            break
        page += 1

    print(f"\n[info] Total participants: {len(all_participants)}", file=sys.stderr)

    import csv

    fieldnames = HackathonParticipant.fieldnames()
    rows = [p.model_dump() for p in all_participants]

    if output:
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"[info] Wrote → {output}", file=sys.stderr)
    else:
        import io
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        print(buf.getvalue())

    if emit_events:
        await emit_hackathon_events(all_participants)


def participants_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-participants --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-participants",
        description="Crawl Devpost hackathon participants page and export to CSV.",
    )
    parser.add_argument(
        "hackathon_url",
        metavar="URL",
        help="Hackathon participants URL (e.g. https://hack-days-niet.devpost.com/participants)",
    )
    parser.add_argument(
        "--jwt",
        metavar="TOKEN",
        default=None,
        help="Value of the _devpost session cookie from your browser. Falls back to DEVPOST_SESSION in .env",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        default=None,
        help="Output CSV file path (default: stdout)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        default=False,
        help="Skip email enrichment (faster)",
    )
    parser.add_argument(
        "--emit-events",
        action="store_true",
        default=False,
        help="Emit devpost_hackathon events to Customer.io (requires CUSTOMERIO_SITE_ID and CUSTOMERIO_API_KEY in .env)",
    )
    args = parser.parse_args()

    if not args.output:
        parsed = urlparse(args.hackathon_url)
        slug = parsed.hostname.split(".")[0] if parsed.hostname else "hackathon"
        args.output = f"{slug}-participants.csv"
        print(f"[info] No -o given, defaulting to {args.output}", file=sys.stderr)

    jwt_token = args.jwt or os.getenv(_PARTICIPANTS_JWT_KEY, "").strip()
    if not jwt_token:
        raise SystemExit(
            "[error] No session cookie. Pass --jwt TOKEN or set DEVPOST_SESSION in .env\n"
            "  Copy the _devpost cookie value from browser DevTools → Application → Cookies"
        )

    # Persist JWT to .env for reuse
    if args.jwt:
        _ENV_FILE.touch(exist_ok=True)
        set_key(str(_ENV_FILE), _PARTICIPANTS_JWT_KEY, args.jwt)

    asyncio.run(
        _run_participants(
            hackathon_url=args.hackathon_url,
            jwt_token=jwt_token,
            output=args.output,
            no_email=args.no_email,
            emit_events=args.emit_events,
        )
    )


# ---------------------------------------------------------------------------
# devpost-github-forks: list forks → same SQLite + get_github_email enrichment
# ---------------------------------------------------------------------------

def _github_fork_source_key(owner: str, repo: str) -> str:
    return f"github:forks:{owner}/{repo}"


def _fork_to_participant(
    owner: str,
    repo: str,
    *,
    login: str,
    full_name: str,
    owner_html_url: str,
) -> HackathonParticipant:
    gh = f"https://github.com/{login}"
    src = _github_fork_source_key(owner, repo)
    title = f"GitHub forks {owner}/{repo}"
    return HackathonParticipant(
        hackathon_url=src,
        hackathon_title=title,
        username=login,
        name=login,
        specialty=full_name,
        profile_url=owner_html_url or gh,
        github_url=gh,
        linkedin_url="",
        email="",
    )


async def _run_github_forks(
    owner: str,
    repo: str,
    *,
    max_forks: int,
    fork_mode: str,
    db_path: str,
    no_email: bool,
    emit_events: bool,
    force_email: bool,
    emit_limit: int = 0,
) -> None:
    from devpost_scraper.db import HarvestDB

    if fork_mode == "top_by_pushed":
        mode = "top_by_pushed"
    elif fork_mode == "first_n":
        mode = "first_n"
    else:
        raise SystemExit(f"[error] Unknown fork mode: {fork_mode!r}")

    db = HarvestDB(db_path)

    print(
        f"[github-forks] Listing forks for {owner}/{repo} "
        f"(max={max_forks}, mode={fork_mode})…",
        file=sys.stderr,
    )
    if fork_mode == "top_by_pushed":
        print(
            "[github-forks] top_by_pushed scans every fork page, then keeps the top N "
            "by last push — this can take many minutes on popular repos.",
            file=sys.stderr,
        )
    try:
        forks = await fetch_repo_forks(
            owner,
            repo,
            max_forks=max_forks,
            mode=mode,
            progress=fork_mode == "top_by_pushed",
        )
    except Exception as exc:
        db.close()
        raise SystemExit(f"[error] Failed to list forks: {exc}") from exc

    print(f"[github-forks] Collected {len(forks)} forks", file=sys.stderr)

    participants = [
        _fork_to_participant(
            owner,
            repo,
            login=f.owner_login,
            full_name=f.full_name,
            owner_html_url=f.owner_html_url,
        )
        for f in forks
    ]

    new_only = db.upsert_participants(participants)
    print(
        f"[github-forks] DB: {len(new_only)} new, "
        f"{len(participants) - len(new_only)} already known",
        file=sys.stderr,
    )

    to_enrich: list[HackathonParticipant] = participants if force_email else new_only

    if not no_email and to_enrich:
        print(
            f"[github-forks] Email enrichment for {len(to_enrich)} accounts "
            f"(concurrency={_FORK_EMAIL_CONCURRENCY})…",
            file=sys.stderr,
        )
        sem = asyncio.Semaphore(_FORK_EMAIL_CONCURRENCY)

        async def _enrich_one(p: HackathonParticipant) -> None:
            async with sem:
                try:
                    email = await get_github_email(p.github_url)
                    p.email = email
                    if email:
                        print(f"  [email] {email} ← {p.username}", file=sys.stderr)
                except Exception as exc:
                    print(f"  [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)

        await asyncio.gather(*(_enrich_one(p) for p in to_enrich))
        db.update_participant_enrichment_batch(to_enrich)

    src = _github_fork_source_key(owner, repo)
    if emit_events and not no_email:
        unemitted = db.get_unemitted_participants(src)
        if unemitted:
            if emit_limit > 0:
                unemitted = unemitted[:emit_limit]
                print(
                    f"[github-forks] --emit-limit {emit_limit}: capping emit to {len(unemitted)} fork owner(s).",
                    file=sys.stderr,
                )
            print(
                f"[github-forks] Emitting Customer.io for {len(unemitted)} fork owners…",
                file=sys.stderr,
            )
            await emit_github_fork_events(unemitted, owner, repo)
            for p in unemitted:
                db.mark_event_emitted(src, p.username)

    fork_total = db._conn.execute(
        "SELECT COUNT(*) FROM participants WHERE hackathon_url=?", (src,)
    ).fetchone()[0]
    fork_with_email = db._conn.execute(
        "SELECT COUNT(*) FROM participants WHERE hackathon_url=? AND email != ''", (src,)
    ).fetchone()[0]
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("[github-forks] Done.", file=sys.stderr)
    print(f"  fork participants in db: {fork_total}", file=sys.stderr)
    print(f"  with email: {fork_with_email}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


def github_forks_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-github-forks --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-github-forks",
        description=(
            "Mine fork owner emails via GitHub API (fork list + get_github_email). "
            "Stores rows in the same harvest SQLite DB as devpost-harvest "
            "(hackathon_url=github:forks:owner/repo)."
        ),
    )
    parser.add_argument(
        "--preset",
        choices=["mem0", "supermemory"],
        default=None,
        help=(
            "mem0 → mem0ai/mem0, top 2000 by pushed_at. "
            "supermemory → supermemoryai/supermemory, first 2000 forks (API order)."
        ),
    )
    parser.add_argument(
        "--repo",
        metavar="OWNER/REPO",
        default=None,
        help="Repository (e.g. mem0ai/mem0). Not needed if --preset is set.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2000,
        metavar="N",
        help="Max forks to process (default: 2000)",
    )
    parser.add_argument(
        "--mode",
        choices=["top_by_pushed", "first_n"],
        default=None,
        help="Fork selection: top_by_pushed = all pages then top N by last push; "
        "first_n = first N in API order (newest forks). Default follows --preset.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        default=False,
        help="Skip GitHub email mining",
    )
    parser.add_argument(
        "--emit-events",
        action="store_true",
        default=False,
        help="Emit Customer.io events for participants with email (this source only)",
    )
    parser.add_argument(
        "--force-email",
        action="store_true",
        default=False,
        help="Run email lookup for every fork owner in this run, not only DB-new rows",
    )
    parser.add_argument(
        "--emit-limit",
        type=int,
        default=0,
        metavar="N",
        help="Cap --emit-events to N participants (0 = all). Useful for testing a single send.",
    )
    parser.add_argument(
        "--emit-unsent",
        action="store_true",
        default=False,
        help=(
            "Skip fork scraping — emit github_fork events for all unsent fork owners "
            "already in the DB (across all tracked repos). "
            "Does not touch Devpost or RB2B rows."
        ),
    )
    args = parser.parse_args()

    if args.emit_unsent:
        asyncio.run(_run_github_forks_unsent(db_path=args.db))
        return

    if args.preset == "mem0":
        owner, repo = "mem0ai", "mem0"
        fork_mode = args.mode or "top_by_pushed"
        limit = args.limit
    elif args.preset == "supermemory":
        owner, repo = "supermemoryai", "supermemory"
        fork_mode = args.mode or "first_n"
        limit = args.limit
    else:
        if not args.repo or "/" not in args.repo:
            raise SystemExit(
                "[error] Set --preset mem0|supermemory or pass --repo owner/repo"
            )
        parts = args.repo.strip().split("/", 1)
        owner, repo = parts[0], parts[1]
        fork_mode = args.mode or "first_n"
        limit = args.limit

    asyncio.run(
        _run_github_forks(
            owner,
            repo,
            max_forks=limit,
            fork_mode=fork_mode,
            db_path=args.db,
            no_email=args.no_email,
            emit_events=args.emit_events,
            force_email=args.force_email,
            emit_limit=args.emit_limit,
        )
    )


# ---------------------------------------------------------------------------
# signalforge-gh-search: search GitHub repos → owner emails → SQLite + emit
# ---------------------------------------------------------------------------

def _github_search_source_key(query: str) -> str:
    """Stable SQLite source key for a GitHub search query."""
    slug = re.sub(r"[^a-z0-9]+", "-", query.lower().strip()).strip("-")
    return f"github:search:{slug}"


def _search_result_to_participant(
    result: dict,
    source_key: str,
    query: str,
) -> HackathonParticipant:
    login = result["owner_login"]
    gh = result["owner_html_url"] or f"https://github.com/{login}"
    return HackathonParticipant(
        hackathon_url=source_key,
        hackathon_title=f"GitHub search: {query}",
        username=login,
        name=login,
        specialty=result.get("full_name", ""),
        profile_url=gh,
        github_url=gh,
        linkedin_url="",
        email="",
    )


async def _run_github_search(
    query: str,
    *,
    max_results: int,
    sort: str,
    db_path: str,
    no_email: bool,
    emit_events: bool,
    force_email: bool,
    emit_limit: int = 0,
) -> None:
    from devpost_scraper.db import HarvestDB

    src = _github_search_source_key(query)
    db = HarvestDB(db_path)

    print(
        f"[gh-search] Searching GitHub repos for '{query}' "
        f"(max={max_results}, sort={sort})…",
        file=sys.stderr,
    )
    try:
        results = await search_github_repos(query, max_results=max_results, sort=sort)
    except Exception as exc:
        db.close()
        raise SystemExit(f"[error] GitHub search failed: {exc}") from exc

    print(f"[gh-search] Found {len(results)} repos", file=sys.stderr)

    # De-dup by owner so each GitHub user appears once
    seen_owners: set[str] = set()
    participants: list[HackathonParticipant] = []
    for r in results:
        login = r.get("owner_login", "")
        if not login or login in seen_owners:
            continue
        # Skip org accounts — they rarely have reachable personal emails
        if r.get("owner_type", "").lower() == "organization":
            continue
        seen_owners.add(login)
        participants.append(_search_result_to_participant(r, src, query))

    print(f"[gh-search] {len(participants)} unique user owners", file=sys.stderr)

    new_only = db.upsert_participants(participants)
    print(
        f"[gh-search] DB: {len(new_only)} new, "
        f"{len(participants) - len(new_only)} already known",
        file=sys.stderr,
    )

    to_enrich: list[HackathonParticipant] = participants if force_email else new_only

    if not no_email and to_enrich:
        print(
            f"[gh-search] Email enrichment for {len(to_enrich)} accounts "
            f"(concurrency={_FORK_EMAIL_CONCURRENCY})…",
            file=sys.stderr,
        )
        sem = asyncio.Semaphore(_FORK_EMAIL_CONCURRENCY)

        async def _enrich_one(p: HackathonParticipant) -> None:
            async with sem:
                try:
                    email = await get_github_email(p.github_url)
                    p.email = email
                    if email:
                        print(f"  [email] {email} ← {p.username}", file=sys.stderr)
                except Exception as exc:
                    print(f"  [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)

        await asyncio.gather(*(_enrich_one(p) for p in to_enrich))
        db.update_participant_enrichment_batch(to_enrich)

    if emit_events and not no_email:
        unemitted = db.get_unemitted_participants(src)
        if unemitted:
            if emit_limit > 0:
                unemitted = unemitted[:emit_limit]
                print(
                    f"[gh-search] --emit-limit {emit_limit}: capping emit to {len(unemitted)} owner(s).",
                    file=sys.stderr,
                )
            print(
                f"[gh-search] Emitting Customer.io for {len(unemitted)} repo owners…",
                file=sys.stderr,
            )
            await emit_github_search_events(unemitted, query)
            for p in unemitted:
                db.mark_event_emitted(src, p.username)

    total = db._conn.execute(
        "SELECT COUNT(*) FROM participants WHERE hackathon_url=?", (src,)
    ).fetchone()[0]
    with_email = db._conn.execute(
        "SELECT COUNT(*) FROM participants WHERE hackathon_url=? AND email != ''", (src,)
    ).fetchone()[0]
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("[gh-search] Done.", file=sys.stderr)
    print(f"  query: {query}", file=sys.stderr)
    print(f"  repo owners in db: {total}", file=sys.stderr)
    print(f"  with email: {with_email}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


async def _run_github_search_unsent(db_path: str) -> None:
    """Emit github_search events for all unsent repo owners across all tracked queries."""
    from collections import defaultdict

    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    total_sent = 0

    search_unsent = db.all_unemitted_search_participants()
    if not search_unsent:
        print("[gh-search] No unsent repo owners.", file=sys.stderr)
        db.close()
        return

    by_query: dict[str, list] = defaultdict(list)
    for p in search_unsent:
        by_query[p.hackathon_url].append(p)

    for src, group in by_query.items():
        # Recover original query from hackathon_title ("GitHub search: <original>")
        original_query = group[0].hackathon_title.removeprefix("GitHub search: ")
        print(
            f"[gh-search] {len(group)} owners (query={original_query!r}) to emit…",
            file=sys.stderr,
        )
        await emit_github_search_events(group, original_query)
        for p in group:
            db.mark_event_emitted(p.hackathon_url, p.username)
        total_sent += len(group)

    print(f"\n[gh-search] Done. {total_sent} total github_search events emitted.", file=sys.stderr)
    db.close()


def github_search_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-gh-search --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-gh-search",
        description=(
            "Search GitHub repos by keyword, collect repo owner emails via the GitHub API, "
            "and store results in the same harvest SQLite DB "
            "(hackathon_url=github:search:<query-slug>)."
        ),
    )
    parser.add_argument(
        "query",
        nargs="?",
        default=None,
        help="GitHub search query (e.g. 'AI memory', 'langchain rag')",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=100,
        metavar="N",
        dest="max_results",
        help="Max number of repos to retrieve (default: 100, GitHub caps at 1000)",
    )
    parser.add_argument(
        "--sort",
        choices=["stars", "forks", "updated"],
        default="stars",
        help="Sort order for GitHub search results (default: stars)",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        default=False,
        help="Skip email enrichment — just store repo owners in the DB",
    )
    parser.add_argument(
        "--force-email",
        action="store_true",
        default=False,
        help="Re-run email enrichment even for owners already in the DB",
    )
    parser.add_argument(
        "--emit-events",
        action="store_true",
        default=False,
        help="Emit github_search events to Customer.io for newly enriched owners",
    )
    parser.add_argument(
        "--emit-limit",
        type=int,
        default=0,
        metavar="N",
        help="Cap --emit-events to N owners (0 = all). Useful for testing.",
    )
    parser.add_argument(
        "--emit-unsent",
        action="store_true",
        default=False,
        help=(
            "Skip search — emit github_search events for all unsent owners "
            "already in the DB (across all tracked queries)."
        ),
    )
    args = parser.parse_args()

    if args.emit_unsent:
        asyncio.run(_run_github_search_unsent(db_path=args.db))
        return

    if not args.query:
        parser.error("A search query is required (or use --emit-unsent)")

    asyncio.run(
        _run_github_search(
            args.query,
            max_results=args.max_results,
            sort=args.sort,
            db_path=args.db,
            no_email=args.no_email,
            emit_events=args.emit_events,
            force_email=args.force_email,
            emit_limit=args.emit_limit,
        )
    )


# ---------------------------------------------------------------------------
# devpost-harvest: walk hackathon listing → scrape participants → delta emit
# ---------------------------------------------------------------------------

async def _run_harvest(
    pages: int,
    jwt_token: str,
    db_path: str,
    no_email: bool,
    emit_events: bool,
    rescrape: bool,
    max_participants: int = 0,
    max_hackathons: int = 0,
    statuses: list[str] | None = None,
) -> None:
    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)

    # Phase 1: discover hackathons
    all_hackathons: list[Hackathon] = []
    for page in range(1, pages + 1):
        print(f"[harvest] Fetching hackathon listing page {page}…", file=sys.stderr)
        data = await list_hackathons(page=page, statuses=statuses)
        batch = data.get("hackathons", [])
        if not batch:
            print(f"[harvest] No hackathons on page {page}, stopping.", file=sys.stderr)
            break
        for raw in batch:
            try:
                h = Hackathon(**raw)
            except Exception as exc:
                # {'id': 27924, 
                # 'title': 'btc/acc', 
                # 'url': 'https://btc-acc-27924.devpost.com', 
                # 'organization_name': None, 
                # 'open_state': 'ended', 
                # 'submission_period_dates': 'Feb 06 - Mar 01, 2026', 
                # 'registrations_count': 16, 'prize_amount': '$0', 
                # 'themes': 'Blockchain, Fintech', 'invite_only': False}       
                print(f"  [warn] hackathon parse failed: {exc}\n\n{raw}\n\n", file=sys.stderr)
                raw = {
                    'id': raw['id'],
                    'title': raw['title'],
                    'url': raw['url'],
                    'organization_name': raw['organization_name'] or '',
                    'open_state': raw['open_state'],
                    'submission_period_dates': raw['submission_period_dates'],
                    'registrations_count': raw['registrations_count'],
                    'prize_amount': raw['prize_amount'],
                    'themes': raw['themes'],
                    'invite_only': raw['invite_only'] or False,
                }
                h = Hackathon(**raw)

            if h.invite_only:
                print(f"  [skip] invite-only: {h.title}", file=sys.stderr)
                continue
            db.upsert_hackathon(h)
            all_hackathons.append(h)
            if max_hackathons and len(all_hackathons) >= max_hackathons:
                break
        print(f"[harvest] Page {page}: {len(batch)} hackathons ({len(all_hackathons)} total)", file=sys.stderr)
        if max_hackathons and len(all_hackathons) >= max_hackathons:
            break

    if not all_hackathons:
        print("[harvest] No hackathons found.", file=sys.stderr)
        db.close()
        return

    # Phase 2: for each hackathon, scrape participants
    total_new = 0
    total_emitted = 0

    for h in all_hackathons:
        already_scraped = db.hackathon_scraped(h.url)
        if already_scraped and h.open_state == "ended":
            print(f"  [skip] {h.title} — ended and already scraped, no new participants possible", file=sys.stderr)
            continue
        if not rescrape and already_scraped:
            print(f"  [cached] {h.title} — already scraped, skipping (use --rescrape to force)", file=sys.stderr)
            continue

        print(f"\n[harvest] {h.title} ({h.url})", file=sys.stderr)
        print(f"  registrations: {h.registrations_count}, state: {h.open_state}", file=sys.stderr)

        # Phase 2a: fast scan — scrape all participant pages (no enrichment)
        participants: list[HackathonParticipant] = []
        ppage = 1
        while True:
            try:
                data = await get_hackathon_participants(h.url, jwt_token, page=ppage)
            except Exception as exc:
                print(f"  [warn] participants fetch failed page {ppage}: {exc}", file=sys.stderr)
                break

            batch = data.get("participants", [])
            has_more = data.get("has_more", False)

            if not batch:
                if ppage == 1:
                    print(f"  [info] No participants found (may need auth)", file=sys.stderr)
                break

            print(f"  [scan] page {ppage}: {len(batch)} participants ({len(participants) + len(batch)} so far)…", file=sys.stderr)

            if max_participants and len(participants) + len(batch) > max_participants:
                batch = batch[:max_participants - len(participants)]
                has_more = False

            for raw in batch:
                participants.append(
                    HackathonParticipant(
                        hackathon_url=h.url,
                        hackathon_title=h.title,
                        username=raw.get("username", ""),
                        name=raw.get("name", ""),
                        specialty=raw.get("specialty", ""),
                        profile_url=raw.get("profile_url", ""),
                    )
                )

            if not has_more:
                break
            ppage += 1

        if not participants:
            db.mark_hackathon_scraped(h.url)
            continue

        print(f"  [scan] {len(participants)} participants across {ppage} pages", file=sys.stderr)

        # Phase 2b: upsert → detect delta
        new_participants = db.upsert_participants(participants)
        total_new += len(new_participants)
        print(f"  [db] {len(new_participants)} new, {len(participants) - len(new_participants)} existing", file=sys.stderr)

        # Phase 2c: email-enrich only the delta
        if new_participants and not no_email:
            print(f"  [enrich] enriching {len(new_participants)} new participants…", file=sys.stderr)
            total_targets = sum(1 for p in new_participants if p.profile_url)
            processed = 0
            for p in new_participants:
                if not p.profile_url:
                    continue
                processed += 1
                try:
                    email_data = await find_participant_email(p.profile_url)
                    p.email = email_data.get("email", "")
                    p.github_url = email_data.get("github_url", "")
                    p.linkedin_url = email_data.get("linkedin_url", "")
                    if p.email:
                        print(
                            f"    [email {processed}/{total_targets}] {p.email} ← {p.username}",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            f"    [email {processed}/{total_targets}] (none) ← {p.username}",
                            file=sys.stderr,
                        )
                    db.update_participant_enrichment(p)
                except Exception as exc:
                    print(f"    [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)

        # Phase 2d: emit events for unemitted participants
        if emit_events:
            unemitted = db.get_unemitted_participants(h.url)
            if unemitted:
                print(f"  [cio] Emitting events for {len(unemitted)} unemitted participants…", file=sys.stderr)
                hack_meta = {h.url: {"submission_period_dates": h.submission_period_dates, "open_state": h.open_state}}
                await emit_hackathon_events(unemitted, hackathon_meta=hack_meta)
                for p in unemitted:
                    db.mark_event_emitted(h.url, p.username)
                total_emitted += len(unemitted)

        db.mark_hackathon_scraped(h.url)

    # Summary
    stats = db.stats()
    print(f"\n{'=' * 60}", file=sys.stderr)
    print(f"[harvest] Done.", file=sys.stderr)
    print(f"  hackathons in db: {stats['hackathons']}", file=sys.stderr)
    print(f"  participants in db: {stats['participants']}", file=sys.stderr)
    print(f"  with email: {stats['with_email']}", file=sys.stderr)
    print(f"  new this run: {total_new}", file=sys.stderr)
    print(f"  events emitted (total): {stats['events_emitted']}", file=sys.stderr)
    if total_emitted:
        print(f"  events emitted (this run): {total_emitted}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


async def _run_emit_unsent(db_path: str) -> None:
    """Emit devpost_hackathon events for all unsent Devpost participants.

    Intentionally scoped to Devpost-only rows (hackathon_url NOT LIKE 'github:forks:%').
    Use signalforge-github-forks --emit-unsent for fork owners.
    """
    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    total_sent = 0

    devpost_unsent = db.all_unemitted_participants()
    if devpost_unsent:
        print(f"[emit-unsent] {len(devpost_unsent)} Devpost participants to emit…", file=sys.stderr)
        hack_meta = db.get_hackathon_meta(list({p.hackathon_url for p in devpost_unsent}))
        await emit_hackathon_events(devpost_unsent, hackathon_meta=hack_meta)
        for p in devpost_unsent:
            db.mark_event_emitted(p.hackathon_url, p.username)
        total_sent += len(devpost_unsent)
    else:
        print("[emit-unsent] No unsent Devpost participants.", file=sys.stderr)

    stats = db.stats()
    print(f"\n[emit-unsent] Done. {total_sent} total events emitted.", file=sys.stderr)
    print(f"  events emitted (total in db): {stats['events_emitted']}", file=sys.stderr)
    db.close()


async def _run_github_forks_unsent(db_path: str) -> None:
    """Emit github_fork events for all unsent fork owners across all tracked repos.

    Intentionally scoped to fork rows only (hackathon_url LIKE 'github:forks:%').
    Use signalforge-harvest --emit-unsent for Devpost participants.
    """
    from collections import defaultdict

    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    total_sent = 0

    fork_unsent = db.all_unemitted_fork_participants()
    if not fork_unsent:
        print("[github-forks] No unsent fork owners.", file=sys.stderr)
        db.close()
        return

    by_repo: dict[str, list] = defaultdict(list)
    for p in fork_unsent:
        by_repo[p.hackathon_url].append(p)

    for src, group in by_repo.items():
        # src = "github:forks:owner/repo"
        repo_slug = src.removeprefix("github:forks:")
        owner, repo = repo_slug.split("/", 1)
        print(
            f"[github-forks] {len(group)} fork owners ({repo_slug}) to emit…",
            file=sys.stderr,
        )
        await emit_github_fork_events(group, owner, repo)
        for p in group:
            db.mark_event_emitted(p.hackathon_url, p.username)
        total_sent += len(group)

    print(f"\n[github-forks] Done. {total_sent} total fork events emitted.", file=sys.stderr)
    db.close()


async def _run_emit_all(db_path: str) -> None:
    """Flush every unsent event across all sources: Devpost, GitHub forks, GitHub search, RB2B."""
    from collections import defaultdict

    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    grand_total = 0

    # ── 1. Devpost hackathon participants ──────────────────────────────────────
    devpost_unsent = db.all_unemitted_participants()
    if devpost_unsent:
        print(f"[emit-all] {len(devpost_unsent)} Devpost participants…", file=sys.stderr)
        hack_meta = db.get_hackathon_meta(list({p.hackathon_url for p in devpost_unsent}))
        await emit_hackathon_events(devpost_unsent, hackathon_meta=hack_meta)
        for p in devpost_unsent:
            db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(devpost_unsent)
    else:
        print("[emit-all] Devpost: nothing to send.", file=sys.stderr)

    # ── 2. GitHub fork owners ──────────────────────────────────────────────────
    fork_unsent = db.all_unemitted_fork_participants()
    if fork_unsent:
        by_repo: dict[str, list] = defaultdict(list)
        for p in fork_unsent:
            by_repo[p.hackathon_url].append(p)
        for src, group in by_repo.items():
            repo_slug = src.removeprefix("github:forks:")
            owner, repo = repo_slug.split("/", 1)
            print(f"[emit-all] {len(group)} fork owners ({repo_slug})…", file=sys.stderr)
            await emit_github_fork_events(group, owner, repo)
            for p in group:
                db.mark_event_emitted(p.hackathon_url, p.username)
            grand_total += len(group)
    else:
        print("[emit-all] GitHub forks: nothing to send.", file=sys.stderr)

    # ── 3. GitHub search repo owners ──────────────────────────────────────────
    search_unsent = db.all_unemitted_search_participants()
    if search_unsent:
        by_query: dict[str, list] = defaultdict(list)
        for p in search_unsent:
            by_query[p.hackathon_url].append(p)
        for src, group in by_query.items():
            original_query = group[0].hackathon_title.removeprefix("GitHub search: ")
            print(f"[emit-all] {len(group)} search owners (query={original_query!r})…", file=sys.stderr)
            await emit_github_search_events(group, original_query)
            for p in group:
                db.mark_event_emitted(p.hackathon_url, p.username)
            grand_total += len(group)
    else:
        print("[emit-all] GitHub search: nothing to send.", file=sys.stderr)

    # ── 4. dev.to challenge submitters ────────────────────────────────────────
    devto_unsent = db.all_unemitted_devto_participants()
    if devto_unsent:
        print(f"[emit-all] {len(devto_unsent)} dev.to submitters…", file=sys.stderr)
        await emit_devto_events(devto_unsent)
        for p in devto_unsent:
            db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(devto_unsent)
    else:
        print("[emit-all] dev.to: nothing to send.", file=sys.stderr)

    # ── 5. RB2B visitors ───────────────────────────────────────────────────────
    rb2b_unsent = db.get_unemitted_rb2b_visitors()
    if rb2b_unsent:
        print(f"[emit-all] {len(rb2b_unsent)} RB2B visitors…", file=sys.stderr)
        await emit_visited_site_events(rb2b_unsent)
        for v in rb2b_unsent:
            db.mark_rb2b_event_emitted(v.visitor_id)
        grand_total += len(rb2b_unsent)
    else:
        print("[emit-all] RB2B: nothing to send.", file=sys.stderr)

    print(f"\n[emit-all] Done. {grand_total} total events emitted.", file=sys.stderr)
    db.close()


def emit_all_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-emit-all",
        description="Flush every unsent Customer.io event across all sources in one shot.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    args = parser.parse_args()
    asyncio.run(_run_emit_all(db_path=args.db))


async def _run_emit_batch(db_path: str, batch_size: int) -> None:
    """Emit up to ``batch_size`` events from each source bucket:

    1. Devpost open / recently-closed  → ``devpost_hackathon``
    2. Devpost old-closed (>30 days)   → ``closed_hackathon``
    3. GitHub fork owners              → ``github_fork``
    4. GitHub search repo owners       → ``github_search``
    5. dev.to challenge submitters     → ``devto_challenge``
    6. RB2B identified visitors        → ``visited_site``
    """
    from collections import defaultdict

    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    grand_total = 0

    # ── 1 & 2: Devpost participants split by hackathon age ────────────────────
    all_devpost = db.all_unemitted_participants()
    if all_devpost:
        hack_meta = db.get_hackathon_meta(list({p.hackathon_url for p in all_devpost}))

        open_recent: list = []
        old_closed: list = []
        for p in all_devpost:
            info = hack_meta.get(p.hackathon_url, {})
            name = select_event_name(
                info.get("submission_period_dates", ""),
                info.get("open_state", ""),
            )
            if name == "closed_hackathon":
                old_closed.append(p)
            else:
                open_recent.append(p)

        # Bucket 1: open / recently-closed
        batch_open = open_recent[:batch_size]
        if batch_open:
            print(
                f"[emit-batch] devpost_hackathon: {len(batch_open)}/{len(open_recent)} queued…",
                file=sys.stderr,
            )
            await emit_hackathon_events(batch_open, hackathon_meta=hack_meta)
            for p in batch_open:
                db.mark_event_emitted(p.hackathon_url, p.username)
            grand_total += len(batch_open)
        else:
            print("[emit-batch] devpost_hackathon: nothing to send.", file=sys.stderr)

        # Bucket 2: old closed
        batch_closed = old_closed[:batch_size]
        if batch_closed:
            print(
                f"[emit-batch] closed_hackathon: {len(batch_closed)}/{len(old_closed)} queued…",
                file=sys.stderr,
            )
            await emit_hackathon_events(batch_closed, hackathon_meta=hack_meta)
            for p in batch_closed:
                db.mark_event_emitted(p.hackathon_url, p.username)
            grand_total += len(batch_closed)
        else:
            print("[emit-batch] closed_hackathon: nothing to send.", file=sys.stderr)
    else:
        print("[emit-batch] Devpost: nothing to send.", file=sys.stderr)

    # ── 3: GitHub fork owners ─────────────────────────────────────────────────
    fork_unsent = db.all_unemitted_fork_participants()
    if fork_unsent:
        batch_forks = fork_unsent[:batch_size]
        print(
            f"[emit-batch] github_fork: {len(batch_forks)}/{len(fork_unsent)} queued…",
            file=sys.stderr,
        )
        by_repo: dict[str, list] = defaultdict(list)
        for p in batch_forks:
            by_repo[p.hackathon_url].append(p)
        for src, group in by_repo.items():
            repo_slug = src.removeprefix("github:forks:")
            owner, repo = repo_slug.split("/", 1)
            await emit_github_fork_events(group, owner, repo)
            for p in group:
                db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(batch_forks)
    else:
        print("[emit-batch] github_fork: nothing to send.", file=sys.stderr)

    # ── 4: GitHub search repo owners ──────────────────────────────────────────
    search_unsent = db.all_unemitted_search_participants()
    if search_unsent:
        batch_search = search_unsent[:batch_size]
        print(
            f"[emit-batch] github_search: {len(batch_search)}/{len(search_unsent)} queued…",
            file=sys.stderr,
        )
        by_query: dict[str, list] = defaultdict(list)
        for p in batch_search:
            by_query[p.hackathon_url].append(p)
        for src, group in by_query.items():
            original_query = group[0].hackathon_title.removeprefix("GitHub search: ")
            await emit_github_search_events(group, original_query)
            for p in group:
                db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(batch_search)
    else:
        print("[emit-batch] github_search: nothing to send.", file=sys.stderr)

    # ── 5: dev.to challenge submitters ────────────────────────────────────────
    devto_unsent = db.all_unemitted_devto_participants()
    if devto_unsent:
        batch_devto = devto_unsent[:batch_size]
        print(
            f"[emit-batch] devto_challenge: {len(batch_devto)}/{len(devto_unsent)} queued…",
            file=sys.stderr,
        )
        await emit_devto_events(batch_devto)
        for p in batch_devto:
            db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(batch_devto)
    else:
        print("[emit-batch] devto_challenge: nothing to send.", file=sys.stderr)

    # ── 6: RB2B visitors ──────────────────────────────────────────────────────
    rb2b_unsent = db.get_unemitted_rb2b_visitors()
    if rb2b_unsent:
        batch_rb2b = rb2b_unsent[:batch_size]
        print(
            f"[emit-batch] visited_site: {len(batch_rb2b)}/{len(rb2b_unsent)} queued…",
            file=sys.stderr,
        )
        await emit_visited_site_events(batch_rb2b)
        for v in batch_rb2b:
            db.mark_rb2b_event_emitted(v.visitor_id)
        grand_total += len(batch_rb2b)
    else:
        print("[emit-batch] visited_site: nothing to send.", file=sys.stderr)

    print(f"\n[emit-batch] Done. {grand_total} total events emitted.", file=sys.stderr)
    db.close()


def emit_batch_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-emit-batch",
        description=(
            "Emit up to --batch-size events from each source bucket "
            "(devpost_hackathon, closed_hackathon, github_fork, visited_site). "
            "Safe to run on a cron — run repeatedly until the queue drains."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        metavar="N",
        help="Max events to emit per source bucket (default: 2000)",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    args = parser.parse_args()
    asyncio.run(_run_emit_batch(db_path=args.db, batch_size=args.batch_size))


async def _run_force_email(db_path: str, concurrency: int = 5, limit: int = 0) -> None:
    """Enrich emails for all participants in the DB that have a profile_url but no email."""
    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    targets = db.get_participants_without_email(limit=limit)

    if not targets:
        print("[force-email] All participants already have emails.", file=sys.stderr)
        db.close()
        return

    print(
        f"[force-email] {len(targets)} participants missing email — enriching "
        f"(concurrency={concurrency})…",
        file=sys.stderr,
    )

    sem = asyncio.Semaphore(concurrency)
    found = 0
    done = 0

    async def _enrich_one(p: HackathonParticipant) -> None:
        nonlocal found, done
        async with sem:
            try:
                if p.hackathon_url.startswith("github:forks:"):
                    # Fork owner — use GitHub API directly
                    email = await get_github_email(p.github_url)
                    p.email = email
                else:
                    # Devpost participant — walk Devpost profile → external links
                    try:
                        email_data = await find_participant_email(p.profile_url)
                        p.email = email_data.get("email", "")
                        p.github_url = email_data.get("github_url", "") or p.github_url
                        p.linkedin_url = email_data.get("linkedin_url", "") or p.linkedin_url
                    except Exception:
                        pass
                    # Fallback: if Devpost profile 404'd/failed and we have a GitHub URL, try that
                    if not p.email and p.github_url:
                        p.email = await get_github_email(p.github_url)
                if p.email:
                    found += 1
                    print(f"  [email] {p.email} ← {p.username}", file=sys.stderr)
            except Exception as exc:
                print(f"  [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)
            finally:
                # Commit each participant immediately so progress survives interruption
                db.update_participant_enrichment(p)
                done += 1
                if done % 50 == 0:
                    print(
                        f"  [progress] {done}/{len(targets)} processed, {found} emails found so far…",
                        file=sys.stderr,
                    )

    await asyncio.gather(*(_enrich_one(p) for p in targets))

    stats = db.stats()
    print(f"\n[force-email] Done. {found}/{len(targets)} emails found.", file=sys.stderr)
    print(f"  with email (total in db): {stats['with_email']}", file=sys.stderr)
    db.close()


def _run_export_linkedin_no_email(db_path: str, output: str | None) -> None:
    """Write a CSV of every participant who has a LinkedIn URL but no email."""
    import csv as _csv
    import sys as _sys

    from devpost_scraper.db import HarvestDB
    from devpost_scraper.models import HackathonParticipant

    db = HarvestDB(db_path)
    rows = db.get_participants_with_linkedin_no_email()
    db.close()

    if not rows:
        print("[export-linkedin] No participants with LinkedIn but no email.", file=_sys.stderr)
        return

    fieldnames = HackathonParticipant.fieldnames()

    if output:
        from pathlib import Path as _Path
        out_path = _Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fh = out_path.open("w", newline="", encoding="utf-8")
        close = True
    else:
        fh = _sys.stdout
        close = False

    writer = _csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for p in rows:
        writer.writerow(p.model_dump())

    if close:
        fh.close()

    dest = output or "stdout"
    print(
        f"[export-linkedin] {len(rows)} participants exported → {dest}",
        file=_sys.stderr,
    )


# ---------------------------------------------------------------------------
# signalforge-devto: walk dev.to challenges, scrape submitters, enrich emails
# ---------------------------------------------------------------------------

async def _run_devto_harvest(
    db_path: str,
    session: str,
    remember_token: str,
    current_user: str,
    no_email: bool,
    emit_events: bool,
    rescrape: bool,
    max_submissions: int = 0,
    states: list[str] | None = None,
) -> None:
    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)

    # Phase 1: discover challenges
    print("[devto] Fetching challenge list from dev.to/challenges…", file=sys.stderr)
    try:
        raw_challenges = await list_devto_challenges(session, remember_token, current_user)
    except Exception as exc:
        raise SystemExit(f"[error] Failed to fetch dev.to challenges: {exc}") from exc

    if not raw_challenges:
        print("[devto] No challenges found. Check cookies.", file=sys.stderr)
        db.close()
        return

    # Filter by state
    wanted_states = set(states) if states else {"active", "previous"}
    challenges = [c for c in raw_challenges if c["state"] in wanted_states]
    print(f"[devto] Found {len(raw_challenges)} total challenges, {len(challenges)} match states {wanted_states}", file=sys.stderr)

    total_new = 0
    total_emitted = 0

    for challenge in challenges:
        challenge_url: str = challenge["url"]
        challenge_state: str = challenge["state"]

        # Phase 2: get submission tag from challenge detail page
        print(f"\n[devto] {challenge['title']} ({challenge_url})", file=sys.stderr)
        try:
            detail = await get_devto_challenge_tag(challenge_url, session, remember_token, current_user)
        except Exception as exc:
            print(f"  [warn] Failed to fetch challenge detail: {exc}", file=sys.stderr)
            continue

        tag = detail.get("tag", "")
        title = detail.get("title", "") or challenge["title"]

        if not tag:
            print(f"  [skip] No submission tag found for {challenge_url}", file=sys.stderr)
            continue

        print(f"  tag: #{tag}", file=sys.stderr)
        db.upsert_devto_challenge(tag, title, challenge_url, challenge_state)

        # Check if already scraped
        already_scraped = db.devto_challenge_scraped(tag)
        if already_scraped and challenge_state == "previous":
            print(f"  [skip] Previous challenge, already scraped.", file=sys.stderr)
            if not rescrape:
                continue
        elif already_scraped and not rescrape:
            print(f"  [cached] Already scraped — use --rescrape to force.", file=sys.stderr)
            continue

        # Phase 3: paginate through /api/articles?tag={tag}
        hackathon_url_key = f"devto:challenge:{tag}"
        participants: list[HackathonParticipant] = []
        page = 1
        seen_usernames: set[str] = set()

        while True:
            try:
                articles = await get_devto_tag_articles(tag, page=page)
            except Exception as exc:
                print(f"  [warn] API error on page {page}: {exc}", file=sys.stderr)
                break

            if not articles:
                break

            print(f"  [scan] page {page}: {len(articles)} articles ({len(participants) + len(articles)} so far)…", file=sys.stderr)

            for art in articles:
                user = art.get("user") or {}
                username: str = (user.get("username") or "").strip()
                if not username or username in seen_usernames:
                    continue
                seen_usernames.add(username)

                name: str = (user.get("name") or username).strip()
                github_username: str = (user.get("github_username") or "").strip()
                github_url = f"https://github.com/{github_username}" if github_username else ""
                article_url: str = (art.get("url") or "").strip()

                participants.append(
                    HackathonParticipant(
                        hackathon_url=hackathon_url_key,
                        hackathon_title=title,
                        username=username,
                        name=name,
                        profile_url=f"https://dev.to/{username}",
                        github_url=github_url,
                        specialty=article_url,
                    )
                )

                if max_submissions and len(participants) >= max_submissions:
                    break

            if len(articles) < 30 or (max_submissions and len(participants) >= max_submissions):
                break
            page += 1

        if not participants:
            db.mark_devto_challenge_scraped(tag)
            continue

        print(f"  [scan] {len(participants)} unique submitters across {page} pages", file=sys.stderr)

        # Phase 4: upsert → detect delta
        new_participants = db.upsert_participants(participants)
        total_new += len(new_participants)
        print(f"  [db] {len(new_participants)} new, {len(participants) - len(new_participants)} existing", file=sys.stderr)

        # Phase 5: email-enrich only the delta
        if new_participants and not no_email:
            print(f"  [enrich] enriching {len(new_participants)} new submitters…", file=sys.stderr)
            total_targets = sum(1 for p in new_participants if p.github_url)
            processed = 0
            for p in new_participants:
                if not p.github_url:
                    continue
                processed += 1
                try:
                    email = await get_github_email(p.github_url)
                    p.email = email
                    if email:
                        print(
                            f"    [email {processed}/{total_targets}] {email} ← {p.username}",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            f"    [email {processed}/{total_targets}] (none) ← {p.username}",
                            file=sys.stderr,
                        )
                    db.update_participant_enrichment(p)
                except Exception as exc:
                    print(f"    [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)

        # Phase 6: emit events
        if emit_events:
            unemitted = db.get_unemitted_participants(hackathon_url_key)
            if unemitted:
                print(f"  [cio] Emitting events for {len(unemitted)} unemitted submitters…", file=sys.stderr)
                await emit_devto_events(unemitted)
                for p in unemitted:
                    db.mark_event_emitted(hackathon_url_key, p.username)
                total_emitted += len(unemitted)

        db.mark_devto_challenge_scraped(tag)

    stats = db.stats()
    print(f"\n{'=' * 60}", file=sys.stderr)
    print(f"[devto] Done.", file=sys.stderr)
    print(f"  participants in db: {stats['participants']}", file=sys.stderr)
    print(f"  with email: {stats['with_email']}", file=sys.stderr)
    print(f"  new this run: {total_new}", file=sys.stderr)
    if total_emitted:
        print(f"  events emitted (this run): {total_emitted}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


def devto_harvest_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-devto --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-devto",
        description=(
            "Walk dev.to challenge listings, scrape all submitters, enrich with emails "
            "via GitHub API, store in SQLite, and emit Customer.io events."
        ),
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        default=False,
        help="Skip email enrichment (much faster)",
    )
    parser.add_argument(
        "--emit-events",
        action="store_true",
        default=False,
        help="Emit Customer.io devto_challenge events for new submitters",
    )
    parser.add_argument(
        "--emit-unsent",
        action="store_true",
        default=False,
        help="Skip scraping — emit events for all unsent dev.to participants in the DB",
    )
    parser.add_argument(
        "--rescrape",
        action="store_true",
        default=False,
        help="Re-scrape challenges that were already scraped in a previous run",
    )
    parser.add_argument(
        "--max-submissions",
        type=int,
        default=0,
        metavar="N",
        help="Cap submissions scraped per challenge (0 = unlimited, default: 0)",
    )
    parser.add_argument(
        "--state",
        action="append",
        choices=["active", "previous", "upcoming"],
        default=None,
        dest="states",
        help="Challenge state filter (repeatable, default: active+previous). e.g. --state active",
    )
    args = parser.parse_args()

    session = os.getenv(_DEVTO_SESSION_KEY, "").strip()
    remember_token = os.getenv(_DEVTO_REMEMBER_KEY, "").strip()
    current_user = os.getenv(_DEVTO_CURRENT_USER_KEY, "").strip()

    if not session:
        raise SystemExit(
            f"[error] No dev.to session cookie.\n"
            f"  Set {_DEVTO_SESSION_KEY} in .env\n"
            f"  (Copy _Devto_Forem_Session from browser DevTools → Application → Cookies → dev.to)"
        )

    if args.emit_unsent:
        async def _emit_unsent() -> None:
            from devpost_scraper.db import HarvestDB
            db = HarvestDB(args.db)
            unsent = db.all_unemitted_devto_participants()
            if unsent:
                print(f"[devto] {len(unsent)} unsent dev.to events…", file=sys.stderr)
                await emit_devto_events(unsent)
                for p in unsent:
                    db.mark_event_emitted(p.hackathon_url, p.username)
            else:
                print("[devto] Nothing to send.", file=sys.stderr)
            db.close()
        asyncio.run(_emit_unsent())
        return

    states = args.states or ["active", "previous"]

    asyncio.run(
        _run_devto_harvest(
            db_path=args.db,
            session=session,
            remember_token=remember_token,
            current_user=current_user,
            no_email=args.no_email,
            emit_events=args.emit_events,
            rescrape=args.rescrape,
            max_submissions=args.max_submissions,
            states=states,
        )
    )


def harvest_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-harvest --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-harvest",
        description=(
            "Walk the Devpost hackathon listing, scrape participants per hackathon, "
            "store in SQLite, and emit Customer.io events for new (delta) participants."
        ),
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Number of hackathon listing pages to fetch (9 hackathons/page, default: 3)",
    )
    parser.add_argument(
        "--jwt",
        metavar="TOKEN",
        default=None,
        help="Value of the _devpost session cookie. Falls back to DEVPOST_SESSION in .env",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        default=False,
        help="Skip email enrichment (much faster)",
    )
    parser.add_argument(
        "--emit-events",
        action="store_true",
        default=False,
        help="Emit Customer.io events for delta participants during scrape",
    )
    parser.add_argument(
        "--emit-unsent",
        action="store_true",
        default=False,
        help="Skip scraping — just emit Customer.io events for all unsent participants in the DB",
    )
    parser.add_argument(
        "--force-email",
        action="store_true",
        default=False,
        help="Skip scraping — enrich emails for all DB participants that have a profile_url but no email yet",
    )
    parser.add_argument(
        "--force-email-limit",
        type=int,
        default=0,
        metavar="N",
        help="Cap --force-email to N participants (0 = all, default: 0)",
    )
    parser.add_argument(
        "--export-linkedin",
        action="store_true",
        default=False,
        help="Skip scraping — export CSV of all participants with a LinkedIn URL but no email",
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="PATH",
        default=None,
        help="Output CSV path for --export-linkedin (default: stdout)",
    )
    parser.add_argument(
        "--rescrape",
        action="store_true",
        default=False,
        help="Re-scrape hackathons that were already scraped in a previous run",
    )
    parser.add_argument(
        "--max-participants",
        type=int,
        default=0,
        metavar="N",
        help="Cap participants scraped per hackathon (0 = unlimited, default: 0)",
    )
    parser.add_argument(
        "--hackathons",
        type=int,
        default=0,
        metavar="N",
        help="Only process the first N hackathons from the listing (0 = all, default: 0)",
    )
    parser.add_argument(
        "--status",
        action="append",
        choices=["open", "ended", "upcoming"],
        default=None,
        dest="statuses",
        help="Hackathon status filter (repeatable, default: open). e.g. --status open --status ended",
    )
    args = parser.parse_args()

    if args.statuses is None:
        args.statuses = ["open"]

    if args.emit_unsent:
        asyncio.run(_run_emit_unsent(db_path=args.db))
        return

    if args.force_email:
        asyncio.run(_run_force_email(db_path=args.db, limit=args.force_email_limit))
        return

    if args.export_linkedin:
        _run_export_linkedin_no_email(db_path=args.db, output=args.output)
        return

    jwt_token = args.jwt or os.getenv(_PARTICIPANTS_JWT_KEY, "").strip()
    if not jwt_token:
        raise SystemExit(
            "[error] No session cookie. Pass --jwt TOKEN or set DEVPOST_SESSION in .env\n"
            "  Copy the _devpost cookie value from browser DevTools → Application → Cookies"
        )

    if args.jwt:
        _ENV_FILE.touch(exist_ok=True)
        set_key(str(_ENV_FILE), _PARTICIPANTS_JWT_KEY, args.jwt)

    asyncio.run(
        _run_harvest(
            pages=args.pages,
            jwt_token=jwt_token,
            db_path=args.db,
            no_email=args.no_email,
            emit_events=args.emit_events,
            rescrape=args.rescrape,
            max_participants=args.max_participants,
            max_hackathons=args.hackathons,
            statuses=args.statuses,
        )
    )


# ---------------------------------------------------------------------------
# devpost-rb2b: import RB2B daily CSVs → SQLite → emit visited_site events
# ---------------------------------------------------------------------------

async def _run_rb2b(
    csv_paths: list[str],
    db_path: str,
    emit_events: bool,
    emit_unsent: bool,
    list_exports: bool = False,
    fetch_latest: bool = False,
    fetch_date: str | None = None,
    rb2b_session: str = "",
    reb2b_uid: str = "",
) -> None:
    import csv as _csv
    import glob as _glob
    import tempfile

    from devpost_scraper.db import HarvestDB

    # --list: show available exports and exit
    if list_exports:
        if not rb2b_session or not reb2b_uid:
            raise SystemExit(
                "[error] --list requires RB2B_SESSION and REB2B_UID in .env\n"
                "  Copy _rb2b_session and _reb2buid from browser DevTools → Application → Cookies → app.rb2b.com"
            )
        try:
            exports = await fetch_rb2b_exports(rb2b_session, reb2b_uid)
        except PermissionError as exc:
            raise SystemExit(f"[error] {exc}") from exc
        if not exports:
            print("[rb2b] No exports found.")
            return
        print(f"{'Filename':<30}  {'Rows':>5}  {'Date'}")
        print("-" * 60)
        for e in exports:
            print(f"  {e['filename']:<28}  {e['row_count']:>5}  {e['date_label']}")
        return

    # --fetch / --fetch-date: download from RB2B and pipe into the importer
    if fetch_latest or fetch_date:
        if not rb2b_session or not reb2b_uid:
            raise SystemExit(
                "[error] --fetch requires RB2B_SESSION and REB2B_UID in .env\n"
                "  Copy _rb2b_session and _reb2buid from browser DevTools → Application → Cookies → app.rb2b.com"
            )
        try:
            exports = await fetch_rb2b_exports(rb2b_session, reb2b_uid)
        except PermissionError as exc:
            raise SystemExit(f"[error] {exc}") from exc
        if not exports:
            raise SystemExit("[error] No exports found on RB2B.")

        if fetch_date:
            matches = [e for e in exports if e["date"] == fetch_date]
            if not matches:
                available = ", ".join(e["date"] for e in exports[:5])
                raise SystemExit(
                    f"[error] No export found for {fetch_date}. "
                    f"Available (recent): {available}"
                )
            target = matches[0]
        else:
            target = exports[0]  # most recent

        print(
            f"[rb2b] Downloading {target['filename']} "
            f"({target['row_count']} rows, {target['date_label']})…",
            file=sys.stderr,
        )
        with tempfile.NamedTemporaryFile(
            suffix=".csv", prefix=f"rb2b_{target['date']}_", delete=False
        ) as tmp:
            tmp_path = tmp.name

        await download_rb2b_export(target["url"], tmp_path)
        print(f"[rb2b] Downloaded → {tmp_path}", file=sys.stderr)
        csv_paths = [tmp_path]

    db = HarvestDB(db_path)

    if emit_unsent:
        pending = db.get_unemitted_rb2b_visitors()
        if not pending:
            print("[rb2b] No unsent identified visitors in DB.", file=sys.stderr)
            db.close()
            return
        print(f"[rb2b] Emitting {len(pending)} unsent visitors…", file=sys.stderr)
        await emit_visited_site_events(pending)
        for v in pending:
            db.mark_rb2b_event_emitted(v.visitor_id)
        stats = db.rb2b_stats()
        print(f"[rb2b] Done. events_emitted total: {stats['events_emitted']}", file=sys.stderr)
        db.close()
        return

    # Expand globs so the user can pass daily_*.csv directly
    expanded: list[str] = []
    for pattern in csv_paths:
        matches = _glob.glob(pattern)
        expanded.extend(sorted(matches) if matches else [pattern])

    if not expanded:
        print("[rb2b] No CSV files found.", file=sys.stderr)
        db.close()
        return

    total_new = 0
    total_emitted = 0

    for path in expanded:
        print(f"[rb2b] Importing {path}…", file=sys.stderr)
        try:
            with open(path, newline="", encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
        except OSError as exc:
            print(f"  [warn] Could not read {path}: {exc}", file=sys.stderr)
            continue

        visitors = [Rb2bVisitor.from_csv_row(r, source_file=path) for r in rows]
        new_visitors = db.upsert_rb2b_visitors(visitors)
        total_new += len(new_visitors)
        print(
            f"  {len(visitors)} rows — {len(new_visitors)} new, "
            f"{len(visitors) - len(new_visitors)} already known",
            file=sys.stderr,
        )

        if emit_events and new_visitors:
            identified = [v for v in new_visitors if v.email]
            if identified:
                print(f"  [cio] Emitting {len(identified)} new identified visitors…", file=sys.stderr)
                sent = await emit_visited_site_events(identified)
                total_emitted += sent
                for v in identified:
                    db.mark_rb2b_event_emitted(v.visitor_id)

    stats = db.rb2b_stats()
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("[rb2b] Done.", file=sys.stderr)
    print(f"  total visitors in db: {stats['total']}", file=sys.stderr)
    print(f"  identified (with email): {stats['identified']}", file=sys.stderr)
    print(f"  events emitted (total): {stats['events_emitted']}", file=sys.stderr)
    print(f"  new this run: {total_new}", file=sys.stderr)
    if total_emitted:
        print(f"  events emitted (this run): {total_emitted}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


def lookup_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    parser = argparse.ArgumentParser(
        prog="signalforge-lookup",
        description="Look up a contact by email, name, or username and show their Devpost context.",
    )
    parser.add_argument(
        "query",
        nargs="+",
        help="Email address, name, or username to search for",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    args = parser.parse_args()

    from devpost_scraper.db import HarvestDB

    db = HarvestDB(args.db)
    conn = db._conn

    terms = " ".join(args.query).strip()
    like = f"%{terms}%"

    rows = conn.execute(
        """SELECT p.hackathon_url, p.hackathon_title, p.username, p.name,
                  p.specialty, p.profile_url, p.github_url, p.linkedin_url, p.email,
                  h.title as h_title
           FROM participants p
           LEFT JOIN hackathons h ON p.hackathon_url = h.url
           WHERE p.email LIKE ? OR p.name LIKE ? OR p.username LIKE ?
           ORDER BY p.first_seen_at ASC
        """,
        (like, like, like),
    ).fetchall()

    db.close()

    if not rows:
        print(f"No results found for: {terms}")
        return

    print(f"\nFound {len(rows)} record(s) for: {terms}\n")
    for r in rows:
        hackathon_title = r["hackathon_title"] or r["h_title"] or r["hackathon_url"]
        print(f"  Hackathon : {hackathon_title}")
        print(f"  URL       : {r['hackathon_url']}")
        print(f"  Name      : {r['name'] or '—'}")
        print(f"  Username  : {r['username']}")
        print(f"  Email     : {r['email'] or '—'}")
        print(f"  Profile   : {r['profile_url'] or '—'}")
        if r["github_url"]:
            print(f"  GitHub    : {r['github_url']}")
        if r["linkedin_url"]:
            print(f"  LinkedIn  : {r['linkedin_url']}")
        if r["specialty"]:
            print(f"  Specialty : {r['specialty']}")
        print()


def rb2b_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-rb2b --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-rb2b",
        description=(
            "Import RB2B visitor CSVs into the harvest SQLite DB and emit "
            "visited_site events to Customer.io for identified visitors."
        ),
    )
    parser.add_argument(
        "csv_files",
        nargs="*",
        metavar="CSV",
        help="One or more RB2B daily export CSV files (globs accepted, e.g. 'daily_*.csv')",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    parser.add_argument(
        "--emit-events",
        action="store_true",
        default=False,
        help="Emit visited_site events to Customer.io for newly imported identified visitors",
    )
    parser.add_argument(
        "--emit-unsent",
        action="store_true",
        default=False,
        help="Skip CSV import — just emit events for all unsent identified visitors in the DB",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        default=False,
        dest="list_exports",
        help="List available exports on app.rb2b.com/profiles/exports (requires RB2B_SESSION + REB2B_UID in .env)",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        default=False,
        dest="fetch_latest",
        help="Download the most recent export from RB2B and import it (requires RB2B_SESSION + REB2B_UID in .env)",
    )
    parser.add_argument(
        "--fetch-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Download the export for a specific date (e.g. 2026-03-31) and import it",
    )
    args = parser.parse_args()

    needs_session = args.list_exports or args.fetch_latest or args.fetch_date
    if not args.emit_unsent and not args.csv_files and not needs_session:
        parser.error("Provide at least one CSV file, --fetch, --fetch-date, --list, or --emit-unsent")

    rb2b_session = os.getenv("RB2B_SESSION", "").strip()
    reb2b_uid = os.getenv("REB2B_UID", "").strip()

    asyncio.run(
        _run_rb2b(
            csv_paths=args.csv_files,
            db_path=args.db,
            emit_events=args.emit_events,
            emit_unsent=args.emit_unsent,
            list_exports=args.list_exports,
            fetch_latest=args.fetch_latest,
            fetch_date=args.fetch_date,
            rb2b_session=rb2b_session,
            reb2b_uid=reb2b_uid,
        )
    )


# ── Auto mode ─────────────────────────────────────────────────────────────────

def _auto_step(n: int, msg: str) -> None:
    bar = "─" * 60
    print(f"\n{bar}\n[auto] Step {n}: {msg}\n{bar}", file=sys.stderr)


async def _run_auto(
    db_path: str,
    fetch_date: str,
    pages: int,
    fork_limit: int,
    no_email: bool,
    jwt_token: str,
) -> None:
    import sqlite3 as _sqlite3

    rb2b_session = os.getenv("RB2B_SESSION", "").strip()
    reb2b_uid = os.getenv("REB2B_UID", "").strip()

    # ── 1. RB2B daily fetch ───────────────────────────────────────────────────
    _auto_step(1, f"RB2B — fetching export for {fetch_date}")
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
        print(f"[auto] RB2B step skipped: {exc}", file=sys.stderr)

    # ── 2. Devpost harvest ────────────────────────────────────────────────────
    _auto_step(2, f"Harvest — open hackathons, {pages} pages")
    await _run_harvest(
        pages=pages,
        jwt_token=jwt_token,
        db_path=db_path,
        no_email=no_email,
        emit_events=False,
        rescrape=False,
        statuses=["open"],
    )

    # ── 3. GitHub forks for every tracked repo ────────────────────────────────
    con = _sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT DISTINCT hackathon_url FROM participants "
        "WHERE hackathon_url LIKE 'github:forks:%'"
    ).fetchall()
    con.close()

    if not rows:
        print(
            "[auto] No GitHub repos tracked yet. "
            "Add one with: signalforge-github-forks --repo owner/repo",
            file=sys.stderr,
        )
    else:
        for i, (hackathon_url,) in enumerate(rows, start=1):
            slug = hackathon_url.removeprefix("github:forks:")
            owner, repo = slug.split("/", 1)
            _auto_step(f"3.{i}", f"GitHub forks — {owner}/{repo} (limit {fork_limit})")
            await _run_github_forks(
                owner,
                repo,
                max_forks=fork_limit,
                fork_mode="first_n",
                db_path=db_path,
                no_email=no_email,
                emit_events=False,
                force_email=False,
            )

    # ── 4. GitHub search — delta email scrape for every tracked query ─────────
    import sqlite3 as _sqlite3b
    con = _sqlite3b.connect(db_path)
    search_rows = con.execute(
        "SELECT DISTINCT hackathon_url, hackathon_title FROM participants "
        "WHERE hackathon_url LIKE 'github:search:%'"
    ).fetchall()
    con.close()

    if not search_rows:
        print(
            "[auto] No GitHub search queries tracked yet. "
            "Add one with: signalforge-gh-search 'your query'",
            file=sys.stderr,
        )
    else:
        for i, (hackathon_url, hackathon_title) in enumerate(search_rows, start=1):
            original_query = (hackathon_title or "").removeprefix("GitHub search: ") \
                or hackathon_url.removeprefix("github:search:")
            _auto_step(f"4.{i}", f"GitHub search — '{original_query}'")
            await _run_github_search(
                original_query,
                max_results=100,
                sort="stars",
                db_path=db_path,
                no_email=no_email,
                emit_events=False,
                force_email=False,
            )

    print(
        "\n[auto] All done. Run emit-unsent on each source to flush the queue:\n"
        "  signalforge-harvest --emit-unsent\n"
        "  signalforge-github-forks --emit-unsent\n"
        "  signalforge-gh-search --emit-unsent\n"
        "  signalforge-rb2b --emit-unsent",
        file=sys.stderr,
    )


def auto_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-auto",
        description=(
            "Full automated daily scrape — no events emitted:\n"
            "  1. RB2B: fetch today's visitor export\n"
            "  2. Harvest: walk open Devpost hackathons\n"
            "  3. Forks: refresh every GitHub repo already in the DB\n"
            "  4. Search: delta-scrape every GitHub search query already in the DB\n\n"
            "After this completes, run --emit-unsent on each source to fire Customer.io events."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=100,
        metavar="N",
        help="Devpost hackathon listing pages to fetch (default: 100)",
    )
    parser.add_argument(
        "--fork-limit",
        type=int,
        default=5000,
        metavar="N",
        help="Max forks to process per GitHub repo (default: 5000)",
    )
    parser.add_argument(
        "--fetch-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="RB2B export date to fetch (default: today)",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        default=False,
        help="Skip email enrichment for both harvest and forks",
    )
    parser.add_argument(
        "--jwt",
        metavar="TOKEN",
        default=None,
        help="Devpost session cookie. Falls back to DEVPOST_SESSION in .env",
    )
    args = parser.parse_args()

    from datetime import date as _date
    fetch_date = args.fetch_date or _date.today().isoformat()

    jwt_token = args.jwt or os.getenv(_PARTICIPANTS_JWT_KEY, "").strip()
    if not jwt_token:
        raise SystemExit(
            "[error] No Devpost session cookie.\n"
            "  Pass --jwt TOKEN or set DEVPOST_SESSION in .env\n"
            "  (Copy the _devpost cookie value from browser DevTools → Application → Cookies)"
        )

    asyncio.run(
        _run_auto(
            db_path=args.db,
            fetch_date=fetch_date,
            pages=args.pages,
            fork_limit=args.fork_limit,
            no_email=args.no_email,
            jwt_token=jwt_token,
        )
    )


async def _run_auto_batch(
    db_path: str,
    fetch_date: str,
    pages: int,
    fork_limit: int,
    no_email: bool,
    jwt_token: str,
    batch_size: int,
) -> None:
    """Run full daily scrape then immediately emit one batch from each source bucket."""
    await _run_auto(
        db_path=db_path,
        fetch_date=fetch_date,
        pages=pages,
        fork_limit=fork_limit,
        no_email=no_email,
        jwt_token=jwt_token,
    )
    print("\n[auto-batch] Scrape done — emitting batch…", file=sys.stderr)
    await _run_emit_batch(db_path=db_path, batch_size=batch_size)


def auto_batch_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-auto-batch",
        description=(
            "All-in-one cron command:\n"
            "  1. RB2B: fetch today's visitor export\n"
            "  2. Harvest: walk open Devpost hackathons\n"
            "  3. Forks: refresh every GitHub repo already in the DB\n"
            "  4. Emit: flush up to --batch-size events from each source bucket\n\n"
            "Schedule this daily and the queue self-manages."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--pages", type=int, default=100, metavar="N",
                        help="Devpost listing pages to fetch (default: 100)")
    parser.add_argument("--fork-limit", type=int, default=5000, metavar="N",
                        help="Max forks per GitHub repo (default: 5000)")
    parser.add_argument("--fetch-date", metavar="YYYY-MM-DD", default=None,
                        help="RB2B export date (default: today)")
    parser.add_argument("--batch-size", type=int, default=2000, metavar="N",
                        help="Max events to emit per source bucket (default: 2000)")
    parser.add_argument("--no-email", action="store_true", default=False,
                        help="Skip email enrichment for harvest and forks")
    parser.add_argument("--jwt", metavar="TOKEN", default=None,
                        help="Devpost session cookie. Falls back to DEVPOST_SESSION in .env")
    args = parser.parse_args()

    from datetime import date as _date
    fetch_date = args.fetch_date or _date.today().isoformat()

    jwt_token = args.jwt or os.getenv(_PARTICIPANTS_JWT_KEY, "").strip()
    if not jwt_token:
        raise SystemExit(
            "[error] No Devpost session cookie.\n"
            "  Pass --jwt TOKEN or set DEVPOST_SESSION in .env\n"
            "  (Copy the _devpost cookie value from browser DevTools → Application → Cookies)"
        )

    asyncio.run(
        _run_auto_batch(
            db_path=args.db,
            fetch_date=fetch_date,
            pages=args.pages,
            fork_limit=args.fork_limit,
            no_email=args.no_email,
            jwt_token=jwt_token,
            batch_size=args.batch_size,
        )
    )


# ════════════════════════════════════════════════════════════════════════════
#  signalforge-assistant — interactive AI analyst REPL
# ════════════════════════════════════════════════════════════════════════════

_SF_ASSISTANT_ID_KEY = "SIGNALFORGE_ASSISTANT_ID"

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
                    "sql": {
                        "type": "string",
                        "description": "A valid SELECT or WITH SQL query.",
                    },
                    "filename": {
                        "type": "string",
                        "description": "Output CSV filename, e.g. 'leads_with_email.csv'.",
                    },
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
                    "repo": {
                        "type": "string",
                        "description": "GitHub repo in 'owner/repo' format, e.g. 'backboard-io/backboard'.",
                    },
                    "max_forks": {
                        "type": "integer",
                        "description": "Max forks to process (default 200).",
                    },
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
                    "date": {
                        "type": "string",
                        "description": "Export date in YYYY-MM-DD format (default: today).",
                    },
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
                    "pages": {
                        "type": "integer",
                        "description": "Hackathon listing pages to fetch (default 1, ~9 hackathons/page).",
                    },
                    "statuses": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Hackathon statuses to include: 'open', 'ended', 'upcoming' (default ['open']).",
                    },
                    "max_participants": {
                        "type": "integer",
                        "description": "Max participants to scrape per hackathon (default 50).",
                    },
                },
                "required": [],
            },
        },
    },
]


# ─── Tool handlers ───────────────────────────────────────────────────────────

async def _sf_get_db_schema(args: dict[str, Any], db_path: str) -> dict[str, Any]:
    import sqlite3 as _sq3
    conn = _sq3.connect(db_path)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
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
            print(f"\r{' ' * 40}\r", end="", flush=True)  # clear progress line
            _sf_render_markdown(full_content)
        elif not had_error:
            event_summary = " → ".join(seen_chunks) if seen_chunks else "no events"
            print(
                f"\r{_YELLOW}⚠  Empty response ({event_summary}). "
                f"Try /reset to start a fresh thread.{_RESET}   "
            )
        return

    print()  # newline after any partial streamed content before tool calls

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
            # Long-running scrape — no spinner, let progress output flow naturally
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
            print(
                f"  {_GREEN}✓ {result['rows_exported']:,} rows → {result['filename']}{_RESET}",
                flush=True,
            )
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
    parser.add_argument(
        "--db",
        metavar="PATH",
        default="devpost_harvest.db",
        help="SQLite database path (default: devpost_harvest.db)",
    )
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


def campaigns_main() -> None:
    """Manage Customer.io campaign actions and email content via the App API."""
    from devpost_scraper.campaigns import cmd_get, cmd_get_actions, cmd_get_campaign, cmd_list_campaigns, cmd_show_campaign, cmd_update, cmd_update_all

    parser = argparse.ArgumentParser(
        prog="signalforge-campaigns",
        description="Sync email HTML with Customer.io campaign actions.",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ── list-campaigns ────────────────────────────────────────────────────────
    sub.add_parser(
        "list-campaigns",
        help="List all campaigns (id, state, name)",
    )

    # ── get-campaign ──────────────────────────────────────────────────────────
    p_gc = sub.add_parser(
        "get-campaign",
        help="Fetch one campaign + all its actions → emails/campaigns/{id}.json",
    )
    p_gc.add_argument("--campaign-id", required=True, metavar="ID",
                      help="Customer.io campaign ID")

    # ── show-campaign ─────────────────────────────────────────────────────────
    p_sc = sub.add_parser(
        "show-campaign",
        help="Print a Mermaid flowchart of a campaign's action graph",
    )
    p_sc.add_argument("--campaign-id", required=True, metavar="ID",
                      help="Customer.io campaign ID (must run get-campaign first)")

    # ── get-actions ───────────────────────────────────────────────────────────
    p_ga = sub.add_parser(
        "get-actions",
        help="Fetch all email actions for a campaign and pair with a local folder of HTML files",
    )
    p_ga.add_argument("--campaign-id", required=True, metavar="ID",
                      help="Customer.io campaign ID")
    p_ga.add_argument("--folder", required=True, metavar="PATH",
                      help="Folder of HTML email files (e.g. emails/closed-hackathon)")
    p_ga.add_argument("--yes", "-y", action="store_true",
                      help="Skip confirmation prompt (for scripting)")

    # ── update-all ────────────────────────────────────────────────────────────
    p_ua = sub.add_parser(
        "update-all",
        help="Push all manifest-linked HTML files for a campaign to cx.io",
    )
    p_ua.add_argument("--campaign-id", required=True, metavar="ID",
                      help="Customer.io campaign ID")

    # ── get ──────────────────────────────────────────────────────────────────
    p_get = sub.add_parser(
        "get",
        help="Fetch one action from cx.io and upsert it into emails/manifest.json",
    )
    p_get.add_argument("--campaign-id", required=True, metavar="ID",
                       help="Customer.io campaign ID")
    p_get.add_argument("--action-id", required=True, metavar="ID",
                       help="Customer.io action ID")
    p_get.add_argument("--file", metavar="PATH", default=None,
                       help="Local HTML file to link this action to (e.g. emails/closed-hackathon/variant-a.html)")

    # ── update ───────────────────────────────────────────────────────────────
    p_update = sub.add_parser(
        "update",
        help="Push a local HTML file's subject + body to cx.io",
    )
    p_update.add_argument("--file", required=True, metavar="PATH",
                          help="Local HTML file listed in emails/manifest.json")

    args = parser.parse_args()
    load_dotenv(_ENV_FILE, override=True)

    try:
        if args.command == "list-campaigns":
            cmd_list_campaigns()
        elif args.command == "get-campaign":
            cmd_get_campaign(campaign_id=args.campaign_id)
        elif args.command == "show-campaign":
            cmd_show_campaign(campaign_id=args.campaign_id)
        elif args.command == "get-actions":
            cmd_get_actions(
                campaign_id=args.campaign_id,
                folder=args.folder,
                yes=args.yes,
            )
        elif args.command == "update-all":
            cmd_update_all(campaign_id=args.campaign_id)
        elif args.command == "get":
            cmd_get(
                campaign_id=args.campaign_id,
                action_id=args.action_id,
                file=args.file,
            )
        elif args.command == "update":
            cmd_update(file=args.file)
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
