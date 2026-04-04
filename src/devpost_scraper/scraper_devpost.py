"""Devpost HTTP scraping: search, hackathon listing, project details, participants, profiles."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

_SEARCH_URL = "https://devpost.com/software/search"
_HACKATHONS_API_URL = "https://devpost.com/api/hackathons"

_JSON_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}
_HTML_HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
    "User-Agent": _JSON_HEADERS["User-Agent"],
}

_DEVPOST_NON_PROFILE_PATHS = {
    "software", "hackathons", "settings", "portfolio", "search",
    "about", "contact", "help", "careers", "login", "register",
}


def _text(el: Any) -> str:
    if el is None:
        return ""
    return el.get_text(strip=True)


async def search_projects(query: str, page: int = 1) -> dict[str, Any]:
    """Search Devpost projects. Returns raw API payload with 'software' list."""
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(
            _SEARCH_URL,
            params={"query": query, "page": page},
            headers=_JSON_HEADERS,
        )
        resp.raise_for_status()
        data = resp.json()

    projects = []
    for item in data.get("software", []):
        built = item.get("built_with") or []
        projects.append(
            {
                "title": item.get("name", ""),
                "tagline": item.get("tagline", ""),
                "url": item.get("url", ""),
                "built_with": ", ".join(built) if isinstance(built, list) else str(built),
                "like_count": item.get("like_count", 0),
            }
        )

    return {
        "projects": projects,
        "total_count": data.get("total_count", 0),
        "page": page,
        "per_page": data.get("per_page", 24),
    }


async def list_hackathons(
    page: int = 1,
    statuses: list[str] | None = None,
) -> dict[str, Any]:
    """Fetch one page of hackathons from the Devpost API.
    Returns {"hackathons": [...], "total_count": int, "per_page": int}.
    """
    if statuses is None:
        statuses = ["open"]

    params: list[tuple[str, str]] = [("page", str(page))]
    for s in statuses:
        params.append(("status[]", s))

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(
            _HACKATHONS_API_URL,
            params=params,
            headers={**_JSON_HEADERS, "Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()

    hackathons = []
    for item in data.get("hackathons", []):
        themes = item.get("themes") or []
        theme_names = ", ".join(t.get("name", "") for t in themes if t.get("name"))

        prize_raw = item.get("prize_amount", "") or ""
        prize_clean = re.sub(r"<[^>]+>", "", prize_raw).strip()

        hackathons.append({
            "id": item.get("id", 0),
            "title": item.get("title", ""),
            "url": item.get("url", "").rstrip("/"),
            "organization_name": item.get("organization_name") or "",
            "open_state": item.get("open_state", ""),
            "submission_period_dates": item.get("submission_period_dates", ""),
            "registrations_count": item.get("registrations_count", 0),
            "prize_amount": prize_clean,
            "themes": theme_names,
            "invite_only": bool(item.get("invite_only")),
        })

    meta = data.get("meta", {})
    return {
        "hackathons": hackathons,
        "total_count": meta.get("total_count", 0),
        "per_page": meta.get("per_page", 9),
    }


async def get_project_details(url: str) -> dict[str, Any]:
    """Fetch a Devpost project page and extract detail fields."""
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(url, headers=_HTML_HEADERS)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")

    title = _text(soup.select_one("h1#app-title") or soup.select_one("h1.app_title"))
    tagline = _text(soup.select_one("p#app-details-header-tagline") or soup.select_one("p.large"))

    summary_el = soup.select_one("div#app-details") or soup.select_one("div.app-details")
    summary = summary_el.get_text(" ", strip=True)[:500] if summary_el else ""

    built_tags = [t.get_text(strip=True) for t in soup.select("span.cp-tag")]
    built_with = ", ".join(built_tags)

    hackathon_name = ""
    hackathon_url = ""
    challenge_link = soup.select_one("a.challenge-link") or soup.select_one("a[href*='/hackathons/']")
    if challenge_link:
        hackathon_name = challenge_link.get_text(strip=True)
        hackathon_url = challenge_link.get("href", "")

    prizes: list[str] = []
    for prize_el in soup.select("div.prize, li.prize, span.prize-name"):
        text = prize_el.get_text(strip=True)
        if text:
            prizes.append(text)

    team_members = soup.select("ul#app-team li, div.software-team-member")
    team_size = str(len(team_members)) if team_members else ""

    return {
        "title": title,
        "tagline": tagline,
        "url": url,
        "summary": summary,
        "built_with": built_with,
        "hackathon_name": hackathon_name,
        "hackathon_url": hackathon_url,
        "prizes": "; ".join(prizes),
        "team_size": team_size,
    }


async def get_author_profile_urls(project_url: str) -> dict[str, Any]:
    """From a Devpost project page, return the author profile URLs."""
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(project_url, headers=_HTML_HEADERS)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")
    profiles: list[str] = []

    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if href.startswith("/"):
            href = f"https://devpost.com{href}"
        parsed = urlparse(href)
        if parsed.netloc not in ("devpost.com", "www.devpost.com"):
            continue
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(path_parts) != 1:
            continue
        slug = path_parts[0]
        if slug in _DEVPOST_NON_PROFILE_PATHS:
            continue
        # Devpost usernames are alphanumeric + dashes, no dots or slashes
        if re.match(r"^[a-zA-Z0-9_\-]+$", slug):
            profiles.append(f"https://devpost.com/{slug}")

    return {"author_profile_urls": list(dict.fromkeys(profiles))}


async def get_profile_external_links(profile_url: str) -> dict[str, Any]:
    """From a Devpost author profile, return external links."""
    from devpost_scraper.scraper_email import _extract_emails

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(profile_url, headers=_HTML_HEADERS)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")
    external: list[str] = []
    emails = _extract_emails(html)

    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        parsed = urlparse(href)
        if parsed.scheme not in ("http", "https"):
            continue
        domain = parsed.netloc.lstrip("www.")
        if domain and domain not in ("devpost.com",):
            external.append(href)

    return {
        "profile_url": profile_url,
        "external_links": list(dict.fromkeys(external)),
        "emails_on_profile": emails,
    }


async def get_hackathon_participants(
    hackathon_url: str,
    jwt_token: str,
    page: int = 1,
) -> dict[str, Any]:
    """
    Fetch one page of participants from a Devpost hackathon participants page.
    Requires a valid Devpost session JWT (sent as _devpost cookie).
    Returns {"participants": [...], "has_more": bool}.
    """
    base = hackathon_url.rstrip("/").removesuffix("/participants")
    url = f"{base}/participants"

    # Devpost serves participant HTML fragments via XHR; plain GET returns empty on page 2+
    headers = {
        **_JSON_HEADERS,
        "Accept": "text/javascript, application/javascript",
        "Cookie": f"_devpost={jwt_token}",
        "Referer": url,
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(url, params={"page": page}, headers=headers)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")

    cards = soup.select("div.participant")

    _CARD_SKIP_CLASSES = {"participant"}

    participants: list[dict[str, Any]] = []
    for card in cards:
        link = card.select_one("a.user-profile-link")
        if not link:
            continue
        profile_href: str = link.get("href", "")
        if profile_href.startswith("/"):
            profile_href = f"https://devpost.com{profile_href}"

        parsed = urlparse(profile_href)
        slug = parsed.path.strip("/").split("/")[0] if parsed.path else ""

        img = card.select_one("img[alt]")
        name = img["alt"].strip() if img and img.get("alt") else slug

        card_classes = [c for c in card.get("class", []) if c not in _CARD_SKIP_CLASSES]
        specialty = card_classes[0].replace("-", " ").title() if card_classes else ""

        participants.append({
            "username": slug,
            "name": name,
            "profile_url": profile_href,
            "specialty": specialty,
        })

    next_link = soup.select_one('a[rel="next"]')
    has_more = next_link is not None

    return {"participants": participants, "has_more": has_more, "page": page}
