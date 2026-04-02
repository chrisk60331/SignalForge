from __future__ import annotations

import logging
import os
import sys

import httpx

import json as _json

from devpost_scraper.models import DevpostHackathonEvent, HackathonParticipant, Rb2bVisitor

logger = logging.getLogger(__name__)

_TRACK_API_URL = "https://track.customer.io/api/v1"
_EVENT_NAME = "devpost_hackathon"


class CustomerIOService:
    """Async Customer.io Track API client (httpx + basic auth)."""

    def __init__(self, site_id: str, api_key: str) -> None:
        self._auth = (site_id, api_key)

    async def identify_user(self, user_id: str, email: str, **attrs: str) -> bool:
        payload = {"email": email, **{k: v for k, v in attrs.items() if v}}
        url = f"{_TRACK_API_URL}/customers/{user_id}"
        async with httpx.AsyncClient() as client:
            resp = await client.put(url, json=payload, auth=self._auth, timeout=10.0)
        if resp.status_code == 200:
            return True
        logger.error("[cio] identify %s → %s %s", user_id, resp.status_code, resp.text)
        return False

    async def track_event(self, user_id: str, event_name: str, data: dict) -> bool:
        url = f"{_TRACK_API_URL}/customers/{user_id}/events"
        payload = {"name": event_name, "data": data}
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, auth=self._auth, timeout=10.0)
        if resp.status_code == 200:
            return True
        logger.error("[cio] track %s/%s → %s %s", user_id, event_name, resp.status_code, resp.text)
        return False


def _build_service() -> CustomerIOService:
    site_id = os.getenv("CUSTOMERIO_SITE_ID", "").strip()
    api_key = os.getenv("CUSTOMERIO_API_KEY", "").strip()
    if not site_id or not api_key:
        raise SystemExit(
            "[error] CUSTOMERIO_SITE_ID and CUSTOMERIO_API_KEY must be set in .env"
        )
    return CustomerIOService(site_id, api_key)


async def emit_hackathon_events(participants: list[HackathonParticipant]) -> None:
    eligible = [p for p in participants if p.email]
    if not eligible:
        print("[cio] No participants with emails — skipping event emission", file=sys.stderr)
        return

    svc = _build_service()
    sent = 0

    for p in eligible:
        event = DevpostHackathonEvent(
            hackathon_url=p.hackathon_url,
            hackathon_title=p.hackathon_title,
            username=p.username,
            name=p.name,
            specialty=p.specialty,
            profile_url=p.profile_url,
            github_url=p.github_url,
            linkedin_url=p.linkedin_url,
        )

        name_parts = p.name.split(maxsplit=1)
        first = name_parts[0] if name_parts else ""
        last = name_parts[1] if len(name_parts) > 1 else ""

        await svc.identify_user(p.email, email=p.email, first_name=first, last_name=last)
        ok = await svc.track_event(p.email, _EVENT_NAME, event.model_dump())
        if ok:
            sent += 1
            print(f"  [cio] {_EVENT_NAME} → {p.email}", file=sys.stderr)
        else:
            print(f"  [cio] FAILED {p.email}", file=sys.stderr)

    print(f"[cio] Emitted {sent}/{len(eligible)} events", file=sys.stderr)


_GITHUB_FORK_EVENT = "github_fork"
_VISITED_SITE_EVENT = "visited_site"


async def emit_github_fork_events(
    participants: list[HackathonParticipant],
    owner: str,
    repo: str,
) -> None:
    """Fire a ``github_fork`` event for each fork owner that has an email.

    Separate from ``devpost_hackathon`` so Customer.io campaigns can use
    fork-appropriate copy (e.g. "I noticed you forked {{event.repo}}…")
    instead of hackathon copy.
    """
    eligible = [p for p in participants if p.email]
    if not eligible:
        print("[cio] No fork owners with emails — skipping event emission", file=sys.stderr)
        return

    svc = _build_service()
    repo_slug = f"{owner}/{repo}"
    repo_url = f"https://github.com/{repo_slug}"
    sent = 0

    for p in eligible:
        name_parts = p.name.split(maxsplit=1)
        first = name_parts[0] if name_parts else ""
        last = name_parts[1] if len(name_parts) > 1 else ""

        await svc.identify_user(p.email, email=p.email, first_name=first, last_name=last)

        data = {
            "repo_name": repo_slug,
            "repo_url": repo_url,
            "fork_url": f"{p.github_url}/{repo}",
            "username": p.username,
            "github_url": p.github_url,
        }
        ok = await svc.track_event(p.email, _GITHUB_FORK_EVENT, data)
        if ok:
            sent += 1
            print(f"  [cio] {_GITHUB_FORK_EVENT} → {p.email}", file=sys.stderr)
        else:
            print(f"  [cio] FAILED {p.email}", file=sys.stderr)

    print(f"[cio] Emitted {sent}/{len(eligible)} {_GITHUB_FORK_EVENT} events", file=sys.stderr)


_GITHUB_SEARCH_EVENT = "github_search"


async def emit_github_search_events(
    participants: list[HackathonParticipant],
    query: str,
) -> None:
    """Fire a ``github_search`` event for each repo owner that has an email.

    Separate event name from fork events so Customer.io campaigns can use
    search-appropriate copy (e.g. "I noticed your repo showed up when I searched for {{event.query}}…").
    """
    eligible = [p for p in participants if p.email]
    if not eligible:
        print("[cio] No repo owners with emails — skipping event emission", file=sys.stderr)
        return

    svc = _build_service()
    sent = 0

    for p in eligible:
        name_parts = p.name.split(maxsplit=1)
        first = name_parts[0] if name_parts else ""
        last = name_parts[1] if len(name_parts) > 1 else ""

        await svc.identify_user(p.email, email=p.email, first_name=first, last_name=last)

        data = {
            "query": query,
            "username": p.username,
            "github_url": p.github_url,
            "repo_full_name": p.specialty,
        }
        ok = await svc.track_event(p.email, _GITHUB_SEARCH_EVENT, data)
        if ok:
            sent += 1
            print(f"  [cio] {_GITHUB_SEARCH_EVENT} → {p.email}", file=sys.stderr)
        else:
            print(f"  [cio] FAILED {p.email}", file=sys.stderr)

    print(f"[cio] Emitted {sent}/{len(eligible)} {_GITHUB_SEARCH_EVENT} events", file=sys.stderr)


async def emit_visited_site_events(visitors: list[Rb2bVisitor]) -> int:
    """Identify + fire visited_site for each RB2B visitor that has an email.

    Returns the count of successfully emitted events.
    """
    eligible = [v for v in visitors if v.email]
    if not eligible:
        print("[cio] No identified RB2B visitors to emit", file=sys.stderr)
        return 0

    svc = _build_service()
    sent = 0

    for v in eligible:
        await svc.identify_user(
            v.email,
            email=v.email,
            first_name=v.first_name,
            last_name=v.last_name,
            company_name=v.company_name,
            linkedin_url=v.linkedin_url,
            title=v.title,
        )

        try:
            page_urls = _json.loads(v.recent_page_urls) if v.recent_page_urls else []
        except (ValueError, TypeError):
            page_urls = []

        data = {
            k: val for k, val in {
                "linkedin_url": v.linkedin_url,
                "company_name": v.company_name,
                "title": v.title,
                "industry": v.industry,
                "employee_count": v.employee_count,
                "estimated_revenue": v.estimated_revenue,
                "city": v.city,
                "state": v.state,
                "website": v.website,
                "last_seen_at": v.rb2b_last_seen_at,
                "first_seen_at": v.rb2b_first_seen_at,
                "most_recent_referrer": v.most_recent_referrer,
                "recent_page_count": v.recent_page_count,
                "recent_page_urls": page_urls,
                "profile_type": v.profile_type,
                "source": "rb2b",
            }.items() if val not in (None, "", [])
        }

        ok = await svc.track_event(v.email, _VISITED_SITE_EVENT, data)
        if ok:
            sent += 1
            print(f"  [cio] {_VISITED_SITE_EVENT} → {v.email}", file=sys.stderr)
        else:
            print(f"  [cio] FAILED {v.email}", file=sys.stderr)

    print(f"[cio] Emitted {sent}/{len(eligible)} visited_site events", file=sys.stderr)
    return sent
