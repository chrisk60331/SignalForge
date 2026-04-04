"""signalforge-lookup — search the DB by email, name, or username."""
from __future__ import annotations

import argparse
import sys

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import _ENV_FILE


def lookup_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    parser = argparse.ArgumentParser(
        prog="signalforge-lookup",
        description="Look up a contact by email, name, or username and show their Devpost context.",
    )
    parser.add_argument("query", nargs="+",
                        help="Email address, name, or username to search for")
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    argcomplete.autocomplete(parser)
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
