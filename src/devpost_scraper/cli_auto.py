"""signalforge-auto / signalforge-auto-batch — full daily automated scrape pipeline."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _PARTICIPANTS_JWT_KEY,
    _DEVTO_SESSION_KEY,
    _DEVTO_REMEMBER_KEY,
    _DEVTO_CURRENT_USER_KEY,
    track_run,
)
from devpost_scraper.cli_devto import _run_devto_harvest
from devpost_scraper.cli_emit import _run_emit_batch
from devpost_scraper.cli_github_forks import _run_github_forks
from devpost_scraper.cli_github_search import _run_github_search
from devpost_scraper.cli_harvest import _run_harvest
from devpost_scraper.cli_hn import _run_hn_harvest
from devpost_scraper.cli_rb2b import _run_rb2b


def _auto_step(n: int | str, msg: str) -> None:
    bar = "─" * 60
    print(f"\n{bar}\n[auto] Step {n}: {msg}\n{bar}", file=sys.stderr)


async def _run_auto(
    db_path: str,
    fetch_date: str,
    pages: int,
    fork_limit: int,
    no_email: bool,
    jwt_token: str,
    devto_session: str = "",
    devto_remember: str = "",
    devto_current_user: str = "",
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
            for _sort in ["top_by_pushed", "first_n"]:
                await _run_github_forks(
                    owner,
                    repo,
                    max_forks=fork_limit,
                    fork_mode=_sort,
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
            for _sort in ["stars", "updated", "forks"]:
                await _run_github_search(
                    original_query,
                    max_results=1000,
                    sort=_sort,
                    db_path=db_path,
                    no_email=no_email,
                    emit_events=False,
                    force_email=False,
                )

    # ── 5. HN Show — scrape all available pages ───────────────────────────────
    _auto_step(5, "HN Show — GitHub-linked posts (all available pages)")
    await _run_hn_harvest(
        pages=100,
        db_path=db_path,
        no_email=no_email,
        emit_events=False,
        force_email=False,
    )

    # ── 6. dev.to challenges — active + previous ──────────────────────────────
    _auto_step(6, "dev.to — active & previous challenges")
    if not devto_session:
        print(
            f"[auto] dev.to step skipped — no session cookie.\n"
            f"  Set {_DEVTO_SESSION_KEY} in .env to enable.",
            file=sys.stderr,
        )
    else:
        try:
            await _run_devto_harvest(
                db_path=db_path,
                session=devto_session,
                remember_token=devto_remember,
                current_user=devto_current_user,
                no_email=no_email,
                emit_events=False,
                rescrape=True,
                states=["active"],
            )
        except SystemExit as exc:
            print(f"[auto] dev.to step failed: {exc}", file=sys.stderr)

    print(
        "\n[auto] All done. Run emit-unsent on each source to flush the queue:\n"
        "  signalforge-harvest --emit-unsent\n"
        "  signalforge-github-forks --emit-unsent\n"
        "  signalforge-gh-search --emit-unsent\n"
        "  signalforge-hn --emit-unsent\n"
        "  signalforge-devto --emit-unsent\n"
        "  signalforge-rb2b --emit-unsent",
        file=sys.stderr,
    )


async def _run_auto_batch(
    db_path: str,
    fetch_date: str,
    pages: int,
    fork_limit: int,
    no_email: bool,
    jwt_token: str,
    batch_size: int,
    devto_session: str = "",
    devto_remember: str = "",
    devto_current_user: str = "",
) -> None:
    """Run full daily scrape then immediately emit one batch from each source bucket."""
    await _run_auto(
        db_path=db_path,
        fetch_date=fetch_date,
        pages=pages,
        fork_limit=fork_limit,
        no_email=no_email,
        jwt_token=jwt_token,
        devto_session=devto_session,
        devto_remember=devto_remember,
        devto_current_user=devto_current_user,
    )
    print("\n[auto-batch] Scrape done — emitting batch…", file=sys.stderr)
    await _run_emit_batch(db_path=db_path, batch_size=batch_size)


@track_run("signalforge-auto")
def auto_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-auto",
        description=(
            "Full automated daily scrape — no events emitted:\n"
            "  1. RB2B: fetch today's visitor export\n"
            "  2. Harvest: walk open Devpost hackathons\n"
            "  3. Forks: refresh every GitHub repo already in the DB\n"
            "  4. Search: delta-scrape every GitHub search query already in the DB\n"
            "  5. HN: scrape all available Show HN GitHub posts\n"
            "  6. dev.to: scrape active & previous challenge submitters\n\n"
            "After this completes, run --emit-unsent on each source to fire Customer.io events."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--pages", type=int, default=1000, metavar="N",
                        help="Devpost hackathon listing pages to fetch (default: 100)")
    parser.add_argument("--fork-limit", type=int, default=5000, metavar="N",
                        help="Max forks to process per GitHub repo (default: 5000)")
    parser.add_argument("--fetch-date", metavar="YYYY-MM-DD", default=None,
                        help="RB2B export date to fetch (default: today)")
    parser.add_argument("--no-email", action="store_true", default=False,
                        help="Skip email enrichment for both harvest and forks")
    parser.add_argument("--jwt", metavar="TOKEN", default=None,
                        help="Devpost session cookie. Falls back to DEVPOST_SESSION in .env")
    argcomplete.autocomplete(parser)
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
            devto_session=os.getenv(_DEVTO_SESSION_KEY, "").strip(),
            devto_remember=os.getenv(_DEVTO_REMEMBER_KEY, "").strip(),
            devto_current_user=os.getenv(_DEVTO_CURRENT_USER_KEY, "").strip(),
        )
    )


@track_run("signalforge-auto-batch")
def auto_batch_main() -> None:
    load_dotenv(_ENV_FILE, override=True)

    parser = argparse.ArgumentParser(
        prog="signalforge-auto-batch",
        description=(
            "All-in-one cron command:\n"
            "  1. RB2B: fetch today's visitor export\n"
            "  2. Harvest: walk open Devpost hackathons\n"
            "  3. Forks: refresh every GitHub repo already in the DB\n"
            "  4. Search: delta-scrape every GitHub search query in the DB\n"
            "  5. HN: scrape all available Show HN GitHub posts\n"
            "  6. dev.to: scrape active & previous challenge submitters\n"
            "  7. Emit: flush up to --batch-size events from each source bucket\n\n"
            "Schedule this daily and the queue self-manages."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--pages", type=int, default=1000, metavar="N",
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
    argcomplete.autocomplete(parser)
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
            devto_session=os.getenv(_DEVTO_SESSION_KEY, "").strip(),
            devto_remember=os.getenv(_DEVTO_REMEMBER_KEY, "").strip(),
            devto_current_user=os.getenv(_DEVTO_CURRENT_USER_KEY, "").strip(),
        )
    )
