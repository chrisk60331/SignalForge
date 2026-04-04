"""dev.to challenge scraper — list challenges, fetch tags, paginate article submissions."""
from __future__ import annotations

import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

from devpost_scraper.scraper_devpost import _HTML_HEADERS

_DEVTO_BASE = "https://dev.to"
_DEVTO_API = "https://dev.to/api"


def _devto_cookies(session: str, remember_token: str = "", current_user: str = "") -> str:
    """Build Cookie header string for dev.to requests."""
    parts = [f"_Devto_Forem_Session={session}"]
    if remember_token:
        parts.append(f"remember_user_token={remember_token}")
    if current_user:
        parts.append(f"current_user={current_user}")
    return "; ".join(parts)


async def list_devto_challenges(
    session: str,
    remember_token: str = "",
    current_user: str = "",
) -> list[dict[str, Any]]:
    """Scrape https://dev.to/challenges and return a list of challenge dicts.

    Each dict: {title, url, state}  where state is "active", "upcoming", or "previous".
    """
    cookie = _devto_cookies(session, remember_token, current_user)
    headers = {**_HTML_HEADERS, "Cookie": cookie, "Accept": "text/html"}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(f"{_DEVTO_BASE}/challenges", headers=headers)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    challenges: list[dict[str, Any]] = []
    seen: set[str] = set()
    current_state = "active"

    for el in soup.find_all(["h2", "h3", "a"]):
        if el.name in ("h2", "h3"):
            text = el.get_text(strip=True).lower()
            if "active" in text:
                current_state = "active"
            elif "previous" in text or "past" in text:
                current_state = "previous"
            elif "launching" in text or "upcoming" in text:
                current_state = "upcoming"
            continue

        href: str = el.get("href", "")
        if not href:
            continue
        # Only follow /challenges/{slug} links
        if not (href.startswith("/challenges/") or "/dev.to/challenges/" in href):
            continue

        if href.startswith("/"):
            href = f"{_DEVTO_BASE}{href}"

        if href in seen:
            continue
        seen.add(href)

        raw_text = el.get_text(" ", strip=True)
        # Challenge card text includes description/badge copy after the title — take first sentence
        title = (raw_text.split("  ")[0].strip() or
                 raw_text.split("\n")[0].strip() or
                 href.rsplit("/", 1)[-1].replace("-", " ").title())
        challenges.append({"title": title, "url": href, "state": current_state})

    return challenges


async def get_devto_challenge_tag(
    challenge_url: str,
    session: str = "",
    remember_token: str = "",
    current_user: str = "",
) -> dict[str, Any]:
    """Fetch a dev.to challenge detail page and extract the submission tag + title.

    Returns: {title, tag, challenge_url}
    """
    cookie = _devto_cookies(session, remember_token, current_user)
    headers = {**_HTML_HEADERS, "Cookie": cookie}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(challenge_url, headers=headers)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    title_el = soup.select_one("h1") or soup.select_one("h2")
    title = title_el.get_text(strip=True) if title_el else ""

    # Look for tag page links: /t/{tag}
    tag_name = ""
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if href.startswith("/t/") and "/" not in href[3:]:
            tag_name = href[3:].strip("/")
            break
        if re.search(r"dev\.to/t/([^/\s]+)$", href):
            m = re.search(r"/t/([^/\s]+)$", href)
            if m:
                tag_name = m.group(1)
                break

    # Fallback: look for #tag mentions in text
    if not tag_name:
        page_text = soup.get_text(" ")
        m = re.search(r"#([a-zA-Z0-9]+challenge[a-zA-Z0-9]*)", page_text, re.IGNORECASE)
        if m:
            tag_name = m.group(1).lower()

    return {"title": title, "tag": tag_name, "challenge_url": challenge_url}


async def get_devto_tag_articles(
    tag: str,
    page: int = 1,
    per_page: int = 30,
) -> list[dict[str, Any]]:
    """Fetch articles for a dev.to tag via the public API.

    Returns list of article dicts. Key fields per article:
      - url, title
      - user.username, user.name, user.github_username
    """
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(
            f"{_DEVTO_API}/articles",
            params={"tag": tag, "page": page, "per_page": per_page},
            headers={
                "Accept": "application/json",
                "User-Agent": _HTML_HEADERS["User-Agent"],
            },
        )
        resp.raise_for_status()
    return resp.json()
