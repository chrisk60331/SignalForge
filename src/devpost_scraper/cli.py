"""Backward-compatibility facade — re-exports every entry-point from the focused cli_* modules.

All pyproject.toml [project.scripts] entries now point directly to those modules,
but any external code that does ``from devpost_scraper.cli import <name>`` will still work.
"""
from devpost_scraper.cli_assistant import assistant_main
from devpost_scraper.cli_auto import auto_batch_main, auto_main
from devpost_scraper.cli_campaigns import campaigns_main
from devpost_scraper.cli_devto import devto_harvest_main
from devpost_scraper.cli_emit import emit_all_main, emit_batch_main
from devpost_scraper.cli_github_forks import github_forks_main
from devpost_scraper.cli_github_search import github_search_main
from devpost_scraper.cli_harvest import harvest_main
from devpost_scraper.cli_hn import hn_harvest_main
from devpost_scraper.cli_lookup import lookup_main
from devpost_scraper.cli_participants import participants_main
from devpost_scraper.cli_rb2b import rb2b_main
from devpost_scraper.cli_report import report_main
from devpost_scraper.cli_run import run_main
from devpost_scraper.cli_search import main
from devpost_scraper.cli_shared import landing_main

__all__ = [
    "landing_main",
    "main",
    "participants_main",
    "harvest_main",
    "github_forks_main",
    "rb2b_main",
    "auto_main",
    "lookup_main",
    "github_search_main",
    "emit_all_main",
    "emit_batch_main",
    "auto_batch_main",
    "assistant_main",
    "campaigns_main",
    "devto_harvest_main",
    "hn_harvest_main",
    "run_main",
    "report_main",
]
