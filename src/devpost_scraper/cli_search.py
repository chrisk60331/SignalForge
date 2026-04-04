"""signalforge-devpost-search — Backboard-powered Devpost project search + CSV export."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

import argcomplete
from dotenv import load_dotenv, set_key

from devpost_scraper.backboard_client import (
    BackboardClientError,
    build_client,
    ensure_assistant,
    run_in_thread,
)
from devpost_scraper.cli_shared import (
    _ENV_FILE,
    _ASSISTANT_ID_KEY,
    _print_landing,
)
from devpost_scraper.csv_export import write_projects
from devpost_scraper.models import DevpostProject
from devpost_scraper.scraper import (
    find_author_email,
    get_project_details,
    search_projects,
)

# The assistant's ONLY job is to search and return raw project URLs.
# Python handles all enrichment directly — no tool loop explosion.
_SYSTEM_PROMPT = """\
You are a Devpost search assistant. Given a search term:

1. Call search_devpost_projects for page 1 and page 2.
2. Deduplicate results by URL.
3. Return ONLY a valid JSON array — no prose, no markdown, no code fences.

Each element: {"title": "...", "tagline": "...", "url": "...", "built_with": "..."}
built_with is a comma-separated string of technology names.
Never call the same tool with the same arguments twice.\
"""

_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "search_devpost_projects",
            "description": "Search Devpost for hackathon projects matching a query.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query term"},
                    "page": {"type": "integer", "description": "Page number (default 1)"},
                },
                "required": ["query"],
            },
        },
    },
]


async def _handle_search(args: dict[str, Any]) -> dict[str, Any]:
    query = args["query"]
    page = int(args.get("page") or 1)
    print(f"  [tool] search_devpost_projects(query={query!r}, page={page})", file=sys.stderr)
    return await search_projects(query=query, page=page)


_TOOL_HANDLERS = {
    "search_devpost_projects": _handle_search,
}


async def _load_or_create_assistant(client: Any) -> str:
    load_dotenv(_ENV_FILE, override=True)
    stored_id = os.getenv(_ASSISTANT_ID_KEY, "").strip()
    if stored_id:
        print(f"[info] Reusing assistant {stored_id}", file=sys.stderr)
        return stored_id

    print("[info] Creating Backboard assistant…", file=sys.stderr)
    aid = await ensure_assistant(
        client,
        assistant_id=None,
        name="devpost-scraper-v3",
        system_prompt=_SYSTEM_PROMPT,
        tools=_TOOLS,
    )
    _ENV_FILE.touch(exist_ok=True)
    set_key(str(_ENV_FILE), _ASSISTANT_ID_KEY, str(aid))
    print(f"[info] Created assistant {aid} — saved to .env", file=sys.stderr)
    return str(aid)


def _parse_search_results(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(
            line for line in raw.splitlines()
            if not line.strip().startswith("```")
        ).strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"[error] Assistant returned invalid JSON: {exc}\n\nRaw:\n{raw}"
        ) from exc
    if not isinstance(data, list):
        raise SystemExit(f"[error] Expected JSON array, got {type(data).__name__}")
    return [item for item in data if isinstance(item, dict) and item.get("url")]


async def _enrich_project(
    item: dict[str, Any],
    search_term: str,
) -> DevpostProject:
    url = item["url"]

    details: dict[str, Any] = {}
    try:
        details = await get_project_details(url=url)
        print(f"  [enrich] details {url}", file=sys.stderr)
    except Exception as exc:
        print(f"  [warn] details failed for {url}: {exc}", file=sys.stderr)

    email_data: dict[str, Any] = {}
    try:
        email_data = await find_author_email(project_url=url)
        if email_data.get("email"):
            print(f"  [email] {email_data['email']} ← {url}", file=sys.stderr)
        else:
            print(f"  [email] (none found) ← {url}", file=sys.stderr)
    except Exception as exc:
        print(f"  [warn] email failed for {url}: {exc}", file=sys.stderr)

    author_urls: list[str] = email_data.get("author_profile_urls", [])

    return DevpostProject(
        search_term=search_term,
        title=details.get("title") or item.get("title", ""),
        tagline=details.get("tagline") or item.get("tagline", ""),
        url=url,
        hackathon_name=details.get("hackathon_name", ""),
        hackathon_url=details.get("hackathon_url", ""),
        summary=details.get("summary", ""),
        built_with=details.get("built_with") or item.get("built_with", ""),
        prizes=details.get("prizes", ""),
        team_size=details.get("team_size", ""),
        author_profile_url=author_urls[0] if author_urls else "",
        email=email_data.get("email", ""),
    )


async def run(search_terms: list[str], output: str | None) -> None:
    load_dotenv(_ENV_FILE, override=True)
    client = build_client()
    assistant_id = await _load_or_create_assistant(client)

    all_projects: list[DevpostProject] = []

    for term in search_terms:
        print(f"\n[info] Searching Devpost for: {term!r}", file=sys.stderr)
        raw = await run_in_thread(
            client,
            assistant_id=assistant_id,
            user_message=(
                f"Search Devpost for: {term!r}\n"
                "Collect page 1 and page 2. Return a JSON array of projects."
            ),
            tool_handlers=_TOOL_HANDLERS,
            llm_provider=os.getenv("BACKBOARD_LLM_PROVIDER", "openai"),
            model_name=os.getenv("BACKBOARD_MODEL", "gpt-4o-mini"),
        )
        items = _parse_search_results(raw)
        print(f"[info] Found {len(items)} projects — enriching…", file=sys.stderr)

        projects: list[DevpostProject] = []
        for item in items:
            project = await _enrich_project(item, search_term=term)
            projects.append(project)

        print(f"[info] Collected {len(projects)} projects for {term!r}", file=sys.stderr)
        all_projects.extend(projects)

    print(f"\n[info] Total projects: {len(all_projects)}", file=sys.stderr)
    write_projects(all_projects, output)
    if output:
        print(f"[info] Wrote → {output}", file=sys.stderr)


def main() -> None:
    if len(sys.argv) == 1:
        _print_landing()
        print("Tip: run `signalforge-devpost-search --help` for full usage.")
    return
    parser = argparse.ArgumentParser(
        prog="signalforge-devpost-search",
        description="Extract Devpost project data and export to CSV.",
    )
    parser.add_argument(
        "search_terms",
        nargs="+",
        metavar="TERM",
        help="One or more search terms to query on Devpost",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        default=None,
        help="Output CSV file path (default: stdout)",
    )
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    asyncio.run(run(search_terms=args.search_terms, output=args.output))
