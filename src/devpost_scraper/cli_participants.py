"""signalforge-participants — scrape one hackathon's participant list and export CSV."""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import os
import sys
from urllib.parse import urlparse

import argcomplete
from dotenv import load_dotenv, set_key

from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _PARTICIPANTS_JWT_KEY,
    _print_landing,
    track_run,
)
from devpost_scraper.customerio import emit_hackathon_events
from devpost_scraper.models import HackathonParticipant
from devpost_scraper.scraper import find_participant_email, get_hackathon_participants


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

    fieldnames = HackathonParticipant.fieldnames()
    rows = [p.model_dump() for p in all_participants]

    if output:
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"[info] Wrote → {output}", file=sys.stderr)
    else:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        print(buf.getvalue())

    if emit_events:
        await emit_hackathon_events(all_participants)


@track_run("signalforge-participants")
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
    argcomplete.autocomplete(parser)
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
