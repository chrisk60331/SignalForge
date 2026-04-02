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
from devpost_scraper.customerio import emit_github_fork_events, emit_github_search_events, emit_hackathon_events, emit_visited_site_events
from devpost_scraper.csv_export import write_projects
from devpost_scraper.models import DevpostProject, Hackathon, HackathonParticipant, Rb2bVisitor
from devpost_scraper.scraper import (
    download_rb2b_export,
    fetch_rb2b_exports,
    fetch_repo_forks,
    find_author_email,
    find_participant_email,
    get_github_email,
    get_hackathon_participants,
    get_project_details,
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

  [1] signalforge              → Search Devpost projects + enrich + export CSV
  [2] signalforge-participants → Scrape hackathon participants + export CSV
  [3] signalforge-harvest      → Walk hackathons → scrape → SQLite → emit events
  [4] signalforge-github-forks → Mine GitHub forks + optional email enrichment
  [5] signalforge-rb2b         → Import RB2B CSVs + emit visited_site events
  [6] signalforge-auto         → Full daily scrape: RB2B + Harvest + all Forks (no emit)
  [7] signalforge-gh-search    → Search GitHub repos by keyword + mine owner emails
"""


def _print_landing() -> None:
    print(_LANDING_BANNER.strip("\n"))
    print()
    print(_LANDING_MENU)

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
        print("Tip: run `signalforge --help` for full usage.")
        return
    parser = argparse.ArgumentParser(
        prog="signalforge",
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
        # src = "github:search:{slug}"
        query_slug = src.removeprefix("github:search:")
        print(
            f"[gh-search] {len(group)} owners (query={query_slug!r}) to emit…",
            file=sys.stderr,
        )
        await emit_github_search_events(group, query_slug)
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
                await emit_hackathon_events(unemitted)
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
        await emit_hackathon_events(devpost_unsent)
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

    print(
        "\n[auto] All done. Run emit-unsent on each source to flush the queue:\n"
        "  signalforge-harvest --emit-unsent\n"
        "  signalforge-github-forks --emit-unsent\n"
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
            "  3. Forks: refresh every GitHub repo already in the DB\n\n"
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
