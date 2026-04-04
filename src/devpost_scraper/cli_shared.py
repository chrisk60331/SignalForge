"""Shared CLI constants, ANSI helpers, landing page, and run-tracking decorator."""
from __future__ import annotations

import functools
import os
import sys
from pathlib import Path

# ── Environment / key constants ───────────────────────────────────────────────
_ENV_FILE = Path(".env")
_ASSISTANT_ID_KEY = "DEVPOST_ASSISTANT_ID"
_SF_ASSISTANT_ID_KEY = "SIGNALFORGE_ASSISTANT_ID"
_PARTICIPANTS_JWT_KEY = "DEVPOST_SESSION"
_DEVTO_SESSION_KEY = "DEV_TO__DEVTO_FOREM_SESSION"
_DEVTO_REMEMBER_KEY = "DEV_TO_REMEMBER_USER_TOKEN"
_DEVTO_CURRENT_USER_KEY = "DEV_TO_CURRENT_USER"

# ── GitHub email enrichment concurrency ───────────────────────────────────────
_FORK_EMAIL_CONCURRENCY = 1  # concurrent GitHub API calls during fork email enrichment

# ── ANSI terminal helpers ──────────────────────────────────────────────────────
_ANSI = sys.stdout.isatty()


def _c(code: str) -> str:
    return code if _ANSI else ""


_RESET   = _c("\033[0m")
_BOLD    = _c("\033[1m")
_DIM     = _c("\033[2m")
_CYAN    = _c("\033[96m")
_YELLOW  = _c("\033[93m")
_GREEN   = _c("\033[92m")
_RED     = _c("\033[91m")
_MAGENTA = _c("\033[95m")

# ── Landing page ──────────────────────────────────────────────────────────────
_LANDING_BANNER = r"""
   _____ _                   ________                    
  / ___/(_)___ _____  ____ _/ / ____/___  _________ ____ 
  \__ \/ / __ `/ __ \/ __ `/ / /_  / __ \/ ___/ __ `/ _ \
 ___/ / / /_/ / / / / /_/ / / __/ / /_/ / /  / /_/ /  __/
/____/_/\__, /_/ /_/\__,_/_/_/    \____/_/   \__, /\___/ 
       /____/                               /____/       
"""
_LANDING_MENU = """\
Command Menu:

  [1]  signalforge-devpost-search  → Search Devpost projects + enrich + export CSV
  [2]  signalforge-participants    → Scrape one hackathon's participants + export CSV
  [3]  signalforge-harvest         → Walk hackathons → scrape → SQLite → emit events
  [4]  signalforge-github-forks    → Mine GitHub fork owners + optional email enrichment
  [5]  signalforge-gh-search       → Search GitHub repos by keyword + mine owner emails
  [6]  signalforge-rb2b            → Import RB2B visitor CSVs + emit visited_site events
  [7]  signalforge-auto            → Full daily scrape: RB2B + Harvest + all Forks + HN Show (no emit)
  [8]  signalforge-auto-batch      → Daily scrape + emit batch in one cron command
  [9]  signalforge-emit-all        → Flush all unsent events across every source at once
  [10] signalforge-emit-batch      → Emit up to --batch-size events per source (cron-friendly)
  [11] signalforge-campaigns       → Sync email HTML with Customer.io campaign actions
   Subcommands: list-campaigns · get-campaign · show-campaign get-actions · update-all · get · update
  [12] signalforge-lookup          → Search the DB by email, name, or username
  [13] signalforge-assistant       → Interactive AI analyst: query DB, export CSV, insights
  [14] signalforge-devto           → Walk dev.to challenges → scrape submitters → SQLite → emit events
  [15] signalforge-hn              → Scrape HN Show posts → filter GitHub → mine emails → emit hacknews_posts events
"""


def _print_landing() -> None:
    print(_LANDING_BANNER.strip("\n"))
    print()
    print(_LANDING_MENU)


def landing_main() -> None:
    _print_landing()


# ── Run tracking ──────────────────────────────────────────────────────────────

def _run_db_path() -> str:
    """Find --db in sys.argv, or fall back to env/default."""
    argv = sys.argv[1:]
    for i, arg in enumerate(argv):
        if arg == "--db" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--db="):
            return arg[5:]
    return os.getenv("SIGNALFORGE_DB", "devpost_harvest.db")


def _finish_run(run_id: int | None, db_path: str, rc: int) -> None:
    if run_id is None:
        return
    from devpost_scraper.db import HarvestDB, _now_iso
    try:
        db = HarvestDB(db_path)
        db.update_run(
            run_id,
            pid=os.getpid(),
            status="done" if rc == 0 else "failed",
            exit_code=rc,
            finished_at=_now_iso(),
        )
        db.close()
    except Exception:
        pass


def track_run(cmd: str):
    """Decorator: record start/finish/failure of a *_main() in the runs table."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            from devpost_scraper.db import HarvestDB, _now_iso
            db_path = _run_db_path()
            run_args = [a for a in sys.argv[1:] if not a.startswith("--db")]
            run_id: int | None = None
            try:
                db = HarvestDB(db_path)
                run_id = db.create_run(cmd, run_args)
                db.update_run(run_id, pid=os.getpid(), status="running")
                db.close()
            except Exception:
                pass
            try:
                fn(*args, **kwargs)
            except SystemExit as exc:
                rc = exc.code if isinstance(exc.code, int) else 1
                _finish_run(run_id, db_path, rc)
                raise
            except Exception:
                _finish_run(run_id, db_path, 1)
                raise
            else:
                _finish_run(run_id, db_path, 0)
        return wrapper
    return decorator
