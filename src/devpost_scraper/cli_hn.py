"""signalforge-hn — scrape HN Show posts, filter GitHub links, mine emails, emit events."""
from __future__ import annotations

import argparse
import asyncio
import sys

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _FORK_EMAIL_CONCURRENCY,
    _print_landing,
    track_run,
)
from devpost_scraper.customerio import emit_hacknews_posts_events
from devpost_scraper.models import HackathonParticipant
from devpost_scraper.scraper import get_github_email, list_hn_show_posts

_HN_SOURCE_KEY = "hn:show"


async def _run_hn_harvest(
    *,
    pages: int,
    db_path: str,
    no_email: bool,
    emit_events: bool,
    force_email: bool,
    emit_limit: int = 0,
) -> None:
    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)

    print(
        f"[hn] Fetching HN Show posts (pages={pages}, GitHub-only)…",
        file=sys.stderr,
    )
    try:
        raw_posts = await list_hn_show_posts(pages=pages)
    except Exception as exc:
        db.close()
        raise SystemExit(f"[error] HN fetch failed: {exc}") from exc

    print(f"[hn] Found {len(raw_posts)} unique GitHub-linked posts", file=sys.stderr)

    participants: list[HackathonParticipant] = []
    for post in raw_posts:
        owner = post["github_url"].rstrip("/").rsplit("/", 1)[-1]
        participants.append(
            HackathonParticipant(
                hackathon_url=_HN_SOURCE_KEY,
                hackathon_title="Hacker News Show HN",
                username=owner,
                name=owner,
                profile_url=post["hn_profile_url"],
                github_url=post["github_url"],
                specialty=post["title"],
                linkedin_url="",
                email="",
            )
        )

    new_only = db.upsert_participants(participants)
    print(
        f"[hn] DB: {len(new_only)} new, {len(participants) - len(new_only)} already known",
        file=sys.stderr,
    )

    to_enrich: list[HackathonParticipant] = participants if force_email else new_only

    if not no_email and to_enrich:
        print(
            f"[hn] Email enrichment for {len(to_enrich)} accounts…",
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
        unemitted = db.get_unemitted_participants(_HN_SOURCE_KEY)
        if unemitted:
            if emit_limit > 0:
                unemitted = unemitted[:emit_limit]
                print(
                    f"[hn] --emit-limit {emit_limit}: capping to {len(unemitted)} person(s).",
                    file=sys.stderr,
                )
            print(
                f"[hn] Emitting Customer.io for {len(unemitted)} posters…",
                file=sys.stderr,
            )
            await emit_hacknews_posts_events(unemitted)
            for p in unemitted:
                db.mark_event_emitted(_HN_SOURCE_KEY, p.username)

    total = db._conn.execute(
        "SELECT COUNT(*) FROM participants WHERE hackathon_url=?", (_HN_SOURCE_KEY,)
    ).fetchone()[0]
    with_email = db._conn.execute(
        "SELECT COUNT(*) FROM participants WHERE hackathon_url=? AND email != ''",
        (_HN_SOURCE_KEY,),
    ).fetchone()[0]
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("[hn] Done.", file=sys.stderr)
    print(f"  source: {_HN_SOURCE_KEY}", file=sys.stderr)
    print(f"  posters in db: {total}", file=sys.stderr)
    print(f"  with email: {with_email}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


async def _run_hn_unsent(db_path: str) -> None:
    from devpost_scraper.db import HarvestDB

    db = HarvestDB(db_path)
    unsent = db.all_unemitted_hn_participants()
    if not unsent:
        print("[hn] No unsent HN posters.", file=sys.stderr)
        db.close()
        return

    print(f"[hn] {len(unsent)} unsent HN posters…", file=sys.stderr)
    await emit_hacknews_posts_events(unsent)
    for p in unsent:
        db.mark_event_emitted(p.hackathon_url, p.username)
    print(f"[hn] Done. {len(unsent)} events emitted.", file=sys.stderr)
    db.close()


@track_run("signalforge-hn")
def hn_harvest_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-hn --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-hn",
        description=(
            "Scrape Hacker News Show HN posts, filter to GitHub links, mine repo owner "
            "emails via GitHub API, store in SQLite, and emit hacknews_posts Customer.io events."
        ),
    )
    parser.add_argument("--pages", type=int, default=1, metavar="N",
                        help="Number of HN Show pages to scrape (default: 1, ~30 posts each)")
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--no-email", action="store_true", default=False,
                        help="Skip email enrichment — just store posters in the DB")
    parser.add_argument("--force-email", action="store_true", default=False,
                        help="Re-run email enrichment even for posters already in the DB")
    parser.add_argument("--emit-events", action="store_true", default=False,
                        help="Emit hacknews_posts events to Customer.io for newly enriched posters")
    parser.add_argument("--emit-limit", type=int, default=0, metavar="N",
                        help="Cap --emit-events to N posters (0 = all). Useful for testing.")
    parser.add_argument("--emit-unsent", action="store_true", default=False,
                        help="Skip scraping — emit hacknews_posts events for all unsent posters in the DB.")
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.emit_unsent:
        asyncio.run(_run_hn_unsent(db_path=args.db))
        return

    asyncio.run(
        _run_hn_harvest(
            pages=args.pages,
            db_path=args.db,
            no_email=args.no_email,
            force_email=args.force_email,
            emit_events=args.emit_events,
            emit_limit=args.emit_limit,
        )
    )
