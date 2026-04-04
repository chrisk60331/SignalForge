"""signalforge-emit-all / signalforge-emit-batch — flush unsent Customer.io events."""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import _ENV_FILE, track_run
from devpost_scraper.customerio import (
    emit_devto_events,
    emit_github_fork_events,
    emit_github_search_events,
    emit_hacknews_posts_events,
    emit_hackathon_events,
    emit_visited_site_events,
    select_event_name,
)


async def _run_emit_all(db_path: str) -> None:
    """Flush every unsent event across all sources: Devpost, GitHub forks, GitHub search, dev.to, HN, RB2B."""
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

    # ── 5. HN Show posters ────────────────────────────────────────────────────
    hn_unsent = db.all_unemitted_hn_participants()
    if hn_unsent:
        print(f"[emit-all] {len(hn_unsent)} HN Show posters…", file=sys.stderr)
        await emit_hacknews_posts_events(hn_unsent)
        for p in hn_unsent:
            db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(hn_unsent)
    else:
        print("[emit-all] HN: nothing to send.", file=sys.stderr)

    # ── 6. RB2B visitors ───────────────────────────────────────────────────────
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


async def _run_emit_batch(db_path: str, batch_size: int) -> None:
    """Emit up to ``batch_size`` events from each source bucket."""
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

        batch_open = open_recent[:batch_size]
        if batch_open:
            print(f"[emit-batch] devpost_hackathon: {len(batch_open)}/{len(open_recent)} queued…", file=sys.stderr)
            await emit_hackathon_events(batch_open, hackathon_meta=hack_meta)
            for p in batch_open:
                db.mark_event_emitted(p.hackathon_url, p.username)
            grand_total += len(batch_open)
        else:
            print("[emit-batch] devpost_hackathon: nothing to send.", file=sys.stderr)

        batch_closed = old_closed[:batch_size]
        if batch_closed:
            print(f"[emit-batch] closed_hackathon: {len(batch_closed)}/{len(old_closed)} queued…", file=sys.stderr)
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
        print(f"[emit-batch] github_fork: {len(batch_forks)}/{len(fork_unsent)} queued…", file=sys.stderr)
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
        print(f"[emit-batch] github_search: {len(batch_search)}/{len(search_unsent)} queued…", file=sys.stderr)
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
        print(f"[emit-batch] devto_challenge: {len(batch_devto)}/{len(devto_unsent)} queued…", file=sys.stderr)
        await emit_devto_events(batch_devto)
        for p in batch_devto:
            db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(batch_devto)
    else:
        print("[emit-batch] devto_challenge: nothing to send.", file=sys.stderr)

    # ── 6: HN Show posters ────────────────────────────────────────────────────
    hn_unsent = db.all_unemitted_hn_participants()
    if hn_unsent:
        batch_hn = hn_unsent[:batch_size]
        print(f"[emit-batch] hacknews_posts: {len(batch_hn)}/{len(hn_unsent)} queued…", file=sys.stderr)
        await emit_hacknews_posts_events(batch_hn)
        for p in batch_hn:
            db.mark_event_emitted(p.hackathon_url, p.username)
        grand_total += len(batch_hn)
    else:
        print("[emit-batch] hacknews_posts: nothing to send.", file=sys.stderr)

    # ── 7: RB2B visitors ──────────────────────────────────────────────────────
    rb2b_unsent = db.get_unemitted_rb2b_visitors()
    if rb2b_unsent:
        batch_rb2b = rb2b_unsent[:batch_size]
        print(f"[emit-batch] visited_site: {len(batch_rb2b)}/{len(rb2b_unsent)} queued…", file=sys.stderr)
        await emit_visited_site_events(batch_rb2b)
        for v in batch_rb2b:
            db.mark_rb2b_event_emitted(v.visitor_id)
        grand_total += len(batch_rb2b)
    else:
        print("[emit-batch] visited_site: nothing to send.", file=sys.stderr)

    print(f"\n[emit-batch] Done. {grand_total} total events emitted.", file=sys.stderr)
    db.close()


@track_run("signalforge-emit-all")
def emit_all_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    parser = argparse.ArgumentParser(
        prog="signalforge-emit-all",
        description="Flush every unsent Customer.io event across all sources in one shot.",
    )
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    asyncio.run(_run_emit_all(db_path=args.db))


@track_run("signalforge-emit-batch")
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
    parser.add_argument("--batch-size", type=int, default=2000, metavar="N",
                        help="Max events to emit per source bucket (default: 2000)")
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    asyncio.run(_run_emit_batch(db_path=args.db, batch_size=args.batch_size))
