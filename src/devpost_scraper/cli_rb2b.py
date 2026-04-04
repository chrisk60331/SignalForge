"""signalforge-rb2b — import RB2B visitor CSVs into SQLite, emit visited_site events."""
from __future__ import annotations

import argparse
import asyncio
import csv as _csv
import glob as _glob
import os
import sys
import tempfile

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import _ENV_FILE, _print_landing, track_run
from devpost_scraper.customerio import emit_visited_site_events
from devpost_scraper.models import Rb2bVisitor
from devpost_scraper.scraper import download_rb2b_export, fetch_rb2b_exports


async def _run_rb2b(
    csv_paths: list[str],
    db_path: str,
    emit_events: bool,
    emit_unsent: bool,
    list_exports: bool = False,
    fetch_latest: bool = False,
    fetch_date: str | None = None,
    rb2b_session: str = "",
    reb2b_uid: str = "",
) -> None:
    from devpost_scraper.db import HarvestDB

    # --list: show available exports and exit
    if list_exports:
        if not rb2b_session or not reb2b_uid:
            raise SystemExit(
                "[error] --list requires RB2B_SESSION and REB2B_UID in .env\n"
                "  Copy _rb2b_session and _reb2buid from browser DevTools → Application → Cookies → app.rb2b.com"
            )
        try:
            exports = await fetch_rb2b_exports(rb2b_session, reb2b_uid)
        except PermissionError as exc:
            raise SystemExit(f"[error] {exc}") from exc
        if not exports:
            print("[rb2b] No exports found.")
            return
        print(f"{'Filename':<30}  {'Rows':>5}  {'Date'}")
        print("-" * 60)
        for e in exports:
            print(f"  {e['filename']:<28}  {e['row_count']:>5}  {e['date_label']}")
        return

    # --fetch / --fetch-date: download from RB2B and pipe into the importer
    if fetch_latest or fetch_date:
        if not rb2b_session or not reb2b_uid:
            raise SystemExit(
                "[error] --fetch requires RB2B_SESSION and REB2B_UID in .env\n"
                "  Copy _rb2b_session and _reb2buid from browser DevTools → Application → Cookies → app.rb2b.com"
            )
        try:
            exports = await fetch_rb2b_exports(rb2b_session, reb2b_uid)
        except PermissionError as exc:
            raise SystemExit(f"[error] {exc}") from exc
        if not exports:
            raise SystemExit("[error] No exports found on RB2B.")

        if fetch_date:
            matches = [e for e in exports if e["date"] == fetch_date]
            if not matches:
                available = ", ".join(e["date"] for e in exports[:5])
                raise SystemExit(
                    f"[error] No export found for {fetch_date}. "
                    f"Available (recent): {available}"
                )
            target = matches[0]
        else:
            target = exports[0]  # most recent

        print(
            f"[rb2b] Downloading {target['filename']} "
            f"({target['row_count']} rows, {target['date_label']})…",
            file=sys.stderr,
        )
        with tempfile.NamedTemporaryFile(
            suffix=".csv", prefix=f"rb2b_{target['date']}_", delete=False
        ) as tmp:
            tmp_path = tmp.name

        await download_rb2b_export(target["url"], tmp_path)
        print(f"[rb2b] Downloaded → {tmp_path}", file=sys.stderr)
        csv_paths = [tmp_path]

    db = HarvestDB(db_path)

    if emit_unsent:
        pending = db.get_unemitted_rb2b_visitors()
        if not pending:
            print("[rb2b] No unsent identified visitors in DB.", file=sys.stderr)
            db.close()
            return
        print(f"[rb2b] Emitting {len(pending)} unsent visitors…", file=sys.stderr)
        await emit_visited_site_events(pending)
        for v in pending:
            db.mark_rb2b_event_emitted(v.visitor_id)
        stats = db.rb2b_stats()
        print(f"[rb2b] Done. events_emitted total: {stats['events_emitted']}", file=sys.stderr)
        db.close()
        return

    # Expand globs so the user can pass daily_*.csv directly
    expanded: list[str] = []
    for pattern in csv_paths:
        matches = _glob.glob(pattern)
        expanded.extend(sorted(matches) if matches else [pattern])

    if not expanded:
        print("[rb2b] No CSV files found.", file=sys.stderr)
        db.close()
        return

    total_new = 0
    total_emitted = 0

    for path in expanded:
        print(f"[rb2b] Importing {path}…", file=sys.stderr)
        try:
            with open(path, newline="", encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
        except OSError as exc:
            print(f"  [warn] Could not read {path}: {exc}", file=sys.stderr)
            continue

        visitors = [Rb2bVisitor.from_csv_row(r, source_file=path) for r in rows]
        new_visitors = db.upsert_rb2b_visitors(visitors)
        total_new += len(new_visitors)
        print(
            f"  {len(visitors)} rows — {len(new_visitors)} new, "
            f"{len(visitors) - len(new_visitors)} already known",
            file=sys.stderr,
        )

        if emit_events and new_visitors:
            identified = [v for v in new_visitors if v.email]
            if identified:
                print(f"  [cio] Emitting {len(identified)} new identified visitors…", file=sys.stderr)
                sent = await emit_visited_site_events(identified)
                total_emitted += sent
                for v in identified:
                    db.mark_rb2b_event_emitted(v.visitor_id)

    stats = db.rb2b_stats()
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("[rb2b] Done.", file=sys.stderr)
    print(f"  total visitors in db: {stats['total']}", file=sys.stderr)
    print(f"  identified (with email): {stats['identified']}", file=sys.stderr)
    print(f"  events emitted (total): {stats['events_emitted']}", file=sys.stderr)
    print(f"  new this run: {total_new}", file=sys.stderr)
    if total_emitted:
        print(f"  events emitted (this run): {total_emitted}", file=sys.stderr)
    print(f"  db: {db_path}", file=sys.stderr)
    db.close()


@track_run("signalforge-rb2b")
def rb2b_main() -> None:
    load_dotenv(_ENV_FILE, override=True)
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-rb2b --help` for full usage.")
        return

    parser = argparse.ArgumentParser(
        prog="signalforge-rb2b",
        description=(
            "Import RB2B visitor CSVs into the harvest SQLite DB and emit "
            "visited_site events to Customer.io for identified visitors."
        ),
    )
    parser.add_argument(
        "csv_files", nargs="*", metavar="CSV",
        help="One or more RB2B daily export CSV files (globs accepted, e.g. 'daily_*.csv')",
    )
    parser.add_argument("--db", metavar="PATH", default="devpost_harvest.db",
                        help="SQLite database path (default: devpost_harvest.db)")
    parser.add_argument("--emit-events", action="store_true", default=False,
                        help="Emit visited_site events to Customer.io for newly imported identified visitors")
    parser.add_argument("--emit-unsent", action="store_true", default=False,
                        help="Skip CSV import — just emit events for all unsent identified visitors in the DB")
    parser.add_argument("--list", action="store_true", default=False, dest="list_exports",
                        help="List available exports on app.rb2b.com/profiles/exports (requires RB2B_SESSION + REB2B_UID in .env)")
    parser.add_argument("--fetch", action="store_true", default=False, dest="fetch_latest",
                        help="Download the most recent export from RB2B and import it (requires RB2B_SESSION + REB2B_UID in .env)")
    parser.add_argument("--fetch-date", metavar="YYYY-MM-DD", default=None,
                        help="Download the export for a specific date (e.g. 2026-03-31) and import it")
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    needs_session = args.list_exports or args.fetch_latest or args.fetch_date
    if not args.emit_unsent and not args.csv_files and not needs_session:
        parser.error("Provide at least one CSV file, --fetch, --fetch-date, --list, or --emit-unsent")

    rb2b_session = os.getenv("RB2B_SESSION", "").strip()
    reb2b_uid = os.getenv("REB2B_UID", "").strip()

    asyncio.run(
        _run_rb2b(
            csv_paths=args.csv_files,
            db_path=args.db,
            emit_events=args.emit_events,
            emit_unsent=args.emit_unsent,
            list_exports=args.list_exports,
            fetch_latest=args.fetch_latest,
            fetch_date=args.fetch_date,
            rb2b_session=rb2b_session,
            reb2b_uid=reb2b_uid,
        )
    )
