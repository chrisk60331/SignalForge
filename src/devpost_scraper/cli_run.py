"""signalforge-run — transparent wrapper: run any signalforge subcommand and track it in SQLite."""
from __future__ import annotations

import argparse
import os
import sys

import argcomplete

from devpost_scraper.cli_shared import _ENV_FILE

_RUN_ALIASES: dict[str, str] = {
    "harvest":      "signalforge-harvest",
    "hn":           "signalforge-hn",
    "gh-search":    "signalforge-gh-search",
    "forks":        "signalforge-github-forks",
    "devto":        "signalforge-devto",
    "rb2b":         "signalforge-rb2b",
    "emit-all":     "signalforge-emit-all",
    "emit-batch":   "signalforge-emit-batch",
    "auto":         "signalforge-auto",
    "auto-batch":   "signalforge-auto-batch",
    "participants": "signalforge-participants",
    "search":       "signalforge-devpost-search",
    "lookup":       "signalforge-lookup",
    "assistant":    "signalforge-assistant",
    "campaigns":    "signalforge-campaigns",
}


def run_main() -> None:
    """Transparent wrapper: run any signalforge subcommand and track it in SQLite."""
    import subprocess

    from devpost_scraper.db import HarvestDB, _now_iso

    parser = argparse.ArgumentParser(
        prog="signalforge-run",
        description=(
            "Run a SignalForge subcommand and record it in the DB.\n\n"
            "Short aliases: harvest, hn, gh-search, forks, devto, rb2b,\n"
            "  emit-all, emit-batch, auto, auto-batch, participants, search,\n"
            "  lookup, assistant, campaigns\n\n"
            "Or pass the full command name (e.g. signalforge-harvest).\n"
            "All remaining arguments are forwarded to the subcommand."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default=os.getenv("SIGNALFORGE_DB", "devpost_harvest.db"),
        metavar="PATH",
        help="SQLite DB path (default: devpost_harvest.db)",
    )
    parser.add_argument("command", help="Subcommand alias or full command name")
    parser.add_argument("args", nargs=argparse.REMAINDER,
                        help="Arguments forwarded to the subcommand")
    argcomplete.autocomplete(parser)
    parsed = parser.parse_args()

    cmd_args = [a for a in (parsed.args or []) if a != "--"]
    full_cmd = _RUN_ALIASES.get(parsed.command, parsed.command)
    display = f"{full_cmd} {' '.join(cmd_args)}".strip()

    db = HarvestDB(parsed.db)
    run_id = db.create_run(full_cmd, cmd_args)

    _dim   = "\033[2m"  if sys.stdout.isatty() else ""
    _reset = "\033[0m"  if sys.stdout.isatty() else ""
    _bold  = "\033[1m"  if sys.stdout.isatty() else ""
    _green = "\033[32m" if sys.stdout.isatty() else ""
    _red   = "\033[31m" if sys.stdout.isatty() else ""

    print(f"{_dim}[run #{run_id}] {display}{_reset}")

    argv = [full_cmd] + cmd_args
    proc = subprocess.Popen(argv)
    db.update_run(run_id, pid=proc.pid, status="running")
    print(f"{_dim}[run #{run_id}] pid {proc.pid}{_reset}\n")

    try:
        rc = proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        try:
            rc = proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = proc.wait()
        db.update_run(run_id, pid=proc.pid, status="interrupted",
                      exit_code=rc, finished_at=_now_iso())
        print(f"\n{_dim}[run #{run_id}] interrupted{_reset}")
        db.close()
        sys.exit(rc)

    status = "done" if rc == 0 else "failed"
    db.update_run(run_id, pid=proc.pid, status=status,
                  exit_code=rc, finished_at=_now_iso())
    label = f"{_green}done{_reset}" if rc == 0 else f"{_red}failed (exit {rc}){_reset}"
    print(f"\n{_dim}[run #{run_id}]{_reset} {_bold}{label}{_reset}")
    db.close()
    sys.exit(rc)
