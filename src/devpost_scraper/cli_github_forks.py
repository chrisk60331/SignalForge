"""signalforge-github-forks — mine fork owners via GitHub API, store in SQLite."""
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
from devpost_scraper.customerio import emit_github_fork_events
from devpost_scraper.models import HackathonParticipant
from devpost_scraper.scraper import fetch_repo_forks, get_github_email


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


async def _run_github_forks_unsent(db_path: str) -> None:
    """Emit github_fork events for all unsent fork owners across all tracked repos."""
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


@track_run("signalforge-github-forks")
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
    argcomplete.autocomplete(parser)
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
