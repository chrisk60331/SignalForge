"""signalforge-harvest — walk Devpost hackathon listing, scrape participants, store in SQLite."""
from __future__ import annotations

import argparse
import asyncio
import csv as _csv
import os
import sys
from pathlib import Path as _Path
from typing import Any

import argcomplete
from dotenv import load_dotenv, set_key

from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _PARTICIPANTS_JWT_KEY,
    _print_landing,
    track_run,
)
from devpost_scraper.customerio import emit_hackathon_events
from devpost_scraper.models import Hackathon, HackathonParticipant
from devpost_scraper.scraper import (
    find_participant_email,
    get_github_email,
    get_hackathon_participants,
    list_hackathons,
)


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
    """Emit devpost_hackathon events for all unsent Devpost participants."""
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
                    email = await get_github_email(p.github_url)
                    p.email = email
                else:
                    try:
                        email_data = await find_participant_email(p.profile_url)
                        p.email = email_data.get("email", "")
                        p.github_url = email_data.get("github_url", "") or p.github_url
                        p.linkedin_url = email_data.get("linkedin_url", "") or p.linkedin_url
                    except Exception:
                        pass
                    if not p.email and p.github_url:
                        p.email = await get_github_email(p.github_url)
                if p.email:
                    found += 1
                    print(f"  [email] {p.email} ← {p.username}", file=sys.stderr)
            except Exception as exc:
                print(f"  [warn] enrich failed for {p.username}: {exc}", file=sys.stderr)
            finally:
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


@track_run("signalforge-harvest")
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
    parser.add_argument("--pages", type=int, default=3,
                        help="Number of hackathon listing pages to fetch (9 hackathons/page, default: 3)")
    parser.add_argument("--jwt", metavar="TOKEN", default=None,
                        help="Value of the _devpost session cookie. Falls back to DEVPOST_SESSION in .env")
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--no-email", action="store_true", default=False,
                        help="Skip email enrichment (much faster)")
    parser.add_argument("--emit-events", action="store_true", default=False,
                        help="Emit Customer.io events for delta participants during scrape")
    parser.add_argument("--emit-unsent", action="store_true", default=False,
                        help="Skip scraping — just emit Customer.io events for all unsent participants in the DB")
    parser.add_argument("--force-email", action="store_true", default=False,
                        help="Skip scraping — enrich emails for all DB participants that have a profile_url but no email yet")
    parser.add_argument("--force-email-limit", type=int, default=0, metavar="N",
                        help="Cap --force-email to N participants (0 = all, default: 0)")
    parser.add_argument("--export-linkedin", action="store_true", default=False,
                        help="Skip scraping — export CSV of all participants with a LinkedIn URL but no email")
    parser.add_argument("--output", "-o", metavar="PATH", default=None,
                        help="Output CSV path for --export-linkedin (default: stdout)")
    parser.add_argument("--rescrape", action="store_true", default=False,
                        help="Re-scrape hackathons that were already scraped in a previous run")
    parser.add_argument("--max-participants", type=int, default=0, metavar="N",
                        help="Cap participants scraped per hackathon (0 = unlimited, default: 0)")
    parser.add_argument("--hackathons", type=int, default=0, metavar="N",
                        help="Only process the first N hackathons from the listing (0 = all, default: 0)")
    parser.add_argument("--status", action="append", choices=["open", "ended", "upcoming"],
                        default=None, dest="statuses",
                        help="Hackathon status filter (repeatable, default: open). e.g. --status open --status ended")
    argcomplete.autocomplete(parser)
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
