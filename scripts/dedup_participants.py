#!/usr/bin/env python3
"""
Deduplicate participants table by email.

Each email should appear at most once. When duplicates exist, we KEEP the row
that best represents "already processed":

  Priority (highest wins):
    1. event_emitted_at IS NOT NULL  — already sent to Customer.io; must not lose this
    2. email IS NOT NULL AND != ''   — enriched row preferred over bare profile
    3. earliest first_seen_at        — keep the original discovery

All other rows for the same email are deleted.

Usage:
    # Dry run — prints affected rows, makes no changes
    python scripts/dedup_participants.py

    # Dry run against a specific DB
    python scripts/dedup_participants.py --db path/to/harvest.db

    # Actually delete the duplicates
    python scripts/dedup_participants.py --execute

    # Execute against a specific DB
    python scripts/dedup_participants.py --db path/to/harvest.db --execute
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def find_duplicates(conn: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    """Return a mapping of email → list of rows (only emails with 2+ rows)."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM participants
        WHERE email IS NOT NULL AND email != ''
        ORDER BY LOWER(email), event_emitted_at DESC NULLS LAST, first_seen_at ASC
        """
    ).fetchall()

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = row["email"].strip().lower()
        grouped.setdefault(key, []).append(row)

    return {email: rows for email, rows in grouped.items() if len(rows) > 1}


def pick_keeper(rows: list[sqlite3.Row]) -> sqlite3.Row:
    """Choose which row to keep for a set of email duplicates."""
    def sort_key(r: sqlite3.Row) -> tuple:
        emitted = 0 if r["event_emitted_at"] else 1          # emitted rows first
        has_email = 0 if (r["email"] and r["email"].strip()) else 1
        return (emitted, has_email, r["first_seen_at"] or "")

    return sorted(rows, key=sort_key)[0]


def run(db_path: str, execute: bool) -> None:
    if not Path(db_path).exists():
        print(f"[error] DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    dupes = find_duplicates(conn)

    if not dupes:
        print("No duplicate emails found — nothing to do.")
        return

    total_to_delete = sum(len(rows) - 1 for rows in dupes.values())
    mode = "DRY RUN" if not execute else "EXECUTE"
    print(f"[{mode}] Found {len(dupes)} email(s) with duplicates → {total_to_delete} row(s) to delete\n")

    rows_to_delete: list[tuple[str, str]] = []  # (hackathon_url, username)

    for email, rows in sorted(dupes.items()):
        keeper = pick_keeper(rows)
        discards = [r for r in rows if not (r["hackathon_url"] == keeper["hackathon_url"] and r["username"] == keeper["username"])]

        print(f"  email: {email}  ({len(rows)} rows → keep 1, delete {len(discards)})")
        print(f"    KEEP   [{keeper['hackathon_url']}] @{keeper['username']}"
              f"  emitted={keeper['event_emitted_at'] or 'no'}"
              f"  first_seen={keeper['first_seen_at']}")
        for d in discards:
            print(f"    DELETE [{d['hackathon_url']}] @{d['username']}"
                  f"  emitted={d['event_emitted_at'] or 'no'}"
                  f"  first_seen={d['first_seen_at']}")
            rows_to_delete.append((d["hackathon_url"], d["username"]))
        print()

    if not execute:
        print(f"[DRY RUN] Would delete {len(rows_to_delete)} row(s). Re-run with --execute to apply.")
        return

    for hackathon_url, username in rows_to_delete:
        conn.execute(
            "DELETE FROM participants WHERE hackathon_url=? AND username=?",
            (hackathon_url, username),
        )
    conn.commit()
    print(f"[EXECUTE] Deleted {len(rows_to_delete)} duplicate row(s).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate participants table by email.")
    parser.add_argument("--db", default="devpost_harvest.db", help="Path to SQLite DB (default: devpost_harvest.db)")
    parser.add_argument("--execute", action="store_true", help="Actually delete rows (default is dry run)")
    args = parser.parse_args()

    run(args.db, args.execute)


if __name__ == "__main__":
    main()
