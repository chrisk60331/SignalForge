"""signalforge-gh-search — search GitHub repos by keyword, mine owner emails, store in SQLite."""
from __future__ import annotations

import argparse
import asyncio
import re
import sys

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _FORK_EMAIL_CONCURRENCY,
    _print_landing,
    track_run,
)
from devpost_scraper.customerio import emit_github_search_events
from devpost_scraper.models import HackathonParticipant
from devpost_scraper.scraper import get_github_email, search_github_repos


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

    seen_owners: set[str] = set()
    participants: list[HackathonParticipant] = []
    for r in results:
        login = r.get("owner_login", "")
        if not login or login in seen_owners:
            continue
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
        _save_lock = asyncio.Lock()
        _pending_save: list[HackathonParticipant] = []
        _GH_SAVE_EVERY = 10

        async def _enrich_one(p: HackathonParticipant) -> None:
            async with sem:
                try:
                    email = await get_github_email(p.github_url)
                    p.email = email
                    if email:
                        print(f"  [email] {email} ← {p.username}", file=sys.stderr)
                except Exception as exc:
                    print(f"  [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)

            async with _save_lock:
                _pending_save.append(p)
                if len(_pending_save) >= _GH_SAVE_EVERY:
                    batch = _pending_save[:]
                    _pending_save.clear()
                    db.update_participant_enrichment_batch(batch)
                    print(
                        f"  [save] {len(batch)} records written to DB "
                        f"({sum(1 for x in batch if x.email)} with email)",
                        file=sys.stderr,
                    )

        await asyncio.gather(*(_enrich_one(p) for p in to_enrich))

        if _pending_save:
            db.update_participant_enrichment_batch(_pending_save)
            print(
                f"  [save] {len(_pending_save)} remaining records written to DB "
                f"({sum(1 for x in _pending_save if x.email)} with email)",
                file=sys.stderr,
            )

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


@track_run("signalforge-gh-search")
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
    argcomplete.autocomplete(parser)
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
