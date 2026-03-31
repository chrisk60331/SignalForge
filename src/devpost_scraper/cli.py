from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv, set_key

from devpost_scraper.backboard_client import (
    BackboardClientError,
    build_client,
    ensure_assistant,
    run_in_thread,
)
from devpost_scraper.customerio import emit_hackathon_events
from devpost_scraper.csv_export import write_projects
from devpost_scraper.models import DevpostProject, Hackathon, HackathonParticipant
from devpost_scraper.scraper import (
    find_author_email,
    find_participant_email,
    get_hackathon_participants,
    get_project_details,
    list_hackathons,
    search_projects,
)

_ENV_FILE = Path(".env")
_ASSISTANT_ID_KEY = "DEVPOST_ASSISTANT_ID"

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
    parser = argparse.ArgumentParser(
        prog="devpost-scraper",
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

    parser = argparse.ArgumentParser(
        prog="devpost-participants",
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
            h = Hackathon(**raw)
            if h.invite_only:
                print(f"  [skip] invite-only: {h.title}", file=sys.stderr)
                continue
            db.upsert_hackathon(h)
            all_hackathons.append(h)
        print(f"[harvest] Page {page}: {len(batch)} hackathons ({len(all_hackathons)} total)", file=sys.stderr)

    if not all_hackathons:
        print("[harvest] No hackathons found.", file=sys.stderr)
        db.close()
        return

    # Phase 2: for each hackathon, scrape participants
    total_new = 0
    total_emitted = 0

    for h in all_hackathons:
        if not rescrape and db.hackathon_scraped(h.url):
            print(f"  [cached] {h.title} — already scraped, skipping (use --rescrape to force)", file=sys.stderr)
            continue

        print(f"\n[harvest] {h.title} ({h.url})", file=sys.stderr)
        print(f"  registrations: {h.registrations_count}, state: {h.open_state}", file=sys.stderr)

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

            print(f"  [page {ppage}] {len(batch)} participants", file=sys.stderr)

            if max_participants and len(participants) + len(batch) > max_participants:
                batch = batch[:max_participants - len(participants)]
                has_more = False

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
                        if email:
                            print(f"    [email] {email} ← {raw.get('username', '')}", file=sys.stderr)
                    except Exception as exc:
                        print(f"    [warn] enrich failed: {exc}", file=sys.stderr)

                participants.append(
                    HackathonParticipant(
                        hackathon_url=h.url,
                        hackathon_title=h.title,
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
            ppage += 1

        if participants:
            new_participants = db.upsert_participants(participants)
            total_new += len(new_participants)
            print(
                f"  [db] {len(participants)} total, {len(new_participants)} new",
                file=sys.stderr,
            )

            if emit_events and new_participants:
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


def harvest_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="devpost-harvest",
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
        help="Emit Customer.io events for delta participants",
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
            statuses=args.statuses,
        )
    )
