from __future__ import annotations

import logging
import os
import sys

import httpx

from devpost_scraper.models import DevpostHackathonEvent, HackathonParticipant

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
