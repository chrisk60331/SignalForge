"""signalforge-devto — walk dev.to challenges, scrape submitters, enrich emails, emit events."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _DEVTO_SESSION_KEY,
    _DEVTO_REMEMBER_KEY,
    _DEVTO_CURRENT_USER_KEY,
    _print_landing,
    track_run,
)
from devpost_scraper.customerio import emit_devto_events
from devpost_scraper.models import HackathonParticipant
from devpost_scraper.scraper import (
    get_devto_challenge_tag,
    get_devto_tag_articles,
    get_github_email,
    list_devto_challenges,
)


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

    wanted_states = set(states) if states else {"active", "previous"}
    challenges = [c for c in raw_challenges if c["state"] in wanted_states]
    print(f"[devto] Found {len(raw_challenges)} total challenges, {len(challenges)} match states {wanted_states}", file=sys.stderr)

    total_new = 0
    total_emitted = 0

    for challenge in challenges:
        challenge_url: str = challenge["url"]
        challenge_state: str = challenge["state"]

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

        already_scraped = db.devto_challenge_scraped(tag)
        if already_scraped and challenge_state == "previous":
            print(f"  [skip] Previous challenge, already scraped.", file=sys.stderr)
            if not rescrape:
                continue
        elif already_scraped and not rescrape:
            print(f"  [cached] Already scraped — use --rescrape to force.", file=sys.stderr)
            continue

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

        new_participants = db.upsert_participants(participants)
        total_new += len(new_participants)
        print(f"  [db] {len(new_participants)} new, {len(participants) - len(new_participants)} existing", file=sys.stderr)

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
                        print(f"    [email {processed}/{total_targets}] {email} ← {p.username}", file=sys.stderr)
                    else:
                        print(f"    [email {processed}/{total_targets}] (none) ← {p.username}", file=sys.stderr)
                    db.update_participant_enrichment(p)
                except Exception as exc:
                    print(f"    [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)

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


@track_run("signalforge-devto")
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
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--no-email", action="store_true", default=False,
                        help="Skip email enrichment (much faster)")
    parser.add_argument("--emit-events", action="store_true", default=False,
                        help="Emit Customer.io devto_challenge events for new submitters")
    parser.add_argument("--emit-unsent", action="store_true", default=False,
                        help="Skip scraping — emit events for all unsent dev.to participants in the DB")
    parser.add_argument("--rescrape", action="store_true", default=False,
                        help="Re-scrape challenges that were already scraped in a previous run")
    parser.add_argument("--max-submissions", type=int, default=0, metavar="N",
                        help="Cap submissions scraped per challenge (0 = unlimited, default: 0)")
    parser.add_argument("--state", action="append", choices=["active", "previous", "upcoming"],
                        default=None, dest="states",
                        help="Challenge state filter (repeatable, default: active+previous). e.g. --state active")
    argcomplete.autocomplete(parser)
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
