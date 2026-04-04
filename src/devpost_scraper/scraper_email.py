"""Email extraction helpers: regex, mailto, link-walking, author/participant chains."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from devpost_scraper.scraper_github import _GITHUB_ORG_PATHS, get_github_email

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Domains we will follow when walking external links from a Devpost profile
_WALKABLE_DOMAINS = {
    "github.com",
    "linktr.ee",
    "bio.link",
    "beacons.ai",
    "linkin.bio",
    "carrd.co",
    "about.me",
    "bento.me",
}

_DEVPOST_OWNED_DOMAINS = {
    "devpost.com", "devpost.team", "info.devpost.com",
    "secure.devpost.com", "d2dmyh35ffsxbl.cloudfront.net",
    "d112y698adiu2z.cloudfront.net",
}

_HTML_HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}


def _extract_emails(html: str) -> list[str]:
    """Find all email addresses in an HTML document (mailto: + bare text)."""
    soup = BeautifulSoup(html, "html.parser")
    found: set[str] = set()

    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if href.startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if addr:
                found.add(addr.lower())

    for match in _EMAIL_RE.finditer(soup.get_text(" ")):
        found.add(match.group().lower())

    # Filter out obviously invalid / placeholder emails
    return [e for e in found if "." in e.split("@")[-1] and len(e) < 80]


def _is_personal_link(url: str) -> bool:
    """Filter out Devpost-owned links (nav, footer, CDN) from external link lists."""
    parsed = urlparse(url)
    domain = parsed.netloc.lstrip("www.")
    return domain not in _DEVPOST_OWNED_DOMAINS


async def extract_emails_from_url(url: str) -> dict[str, Any]:
    """Fetch any URL and return email addresses found on the page."""
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=_HTML_HEADERS)
            resp.raise_for_status()
            html = resp.text
    except Exception as exc:
        return {"url": url, "emails": [], "error": str(exc)}

    emails = _extract_emails(html)
    return {"url": url, "emails": emails}


async def find_author_email(project_url: str) -> dict[str, Any]:
    """
    Full chain: project page → author profile(s) → external links → emails.
    Returns the first email found along with the chain of URLs walked.
    """
    from devpost_scraper.scraper_devpost import get_author_profile_urls, get_profile_external_links

    result: dict[str, Any] = {
        "project_url": project_url,
        "author_profile_urls": [],
        "external_links_walked": [],
        "email": "",
    }

    # Step 1: get author profiles from project page
    profiles_data = await get_author_profile_urls(project_url)
    author_urls: list[str] = profiles_data.get("author_profile_urls", [])
    result["author_profile_urls"] = author_urls

    all_emails: list[str] = []

    for profile_url in author_urls[:3]:  # cap at 3 authors
        profile_data = await get_profile_external_links(profile_url)

        # Emails directly on profile page
        all_emails.extend(profile_data.get("emails_on_profile", []))

        # Walk external links from profile
        for link in profile_data.get("external_links", []):
            parsed = urlparse(link)
            domain = parsed.netloc.lstrip("www.")
            if domain not in _WALKABLE_DOMAINS:
                continue
            result["external_links_walked"].append(link)
            link_data = await extract_emails_from_url(link)
            all_emails.extend(link_data.get("emails", []))

        if all_emails:
            break  # stop after first author with a result

    result["email"] = all_emails[0] if all_emails else ""
    return result


async def find_participant_email(profile_url: str) -> dict[str, Any]:
    """
    Enrich a participant from their Devpost profile:
    1. Extract GitHub URL, LinkedIn URL from profile social links
    2. Try GitHub API for public email
    3. Walk other external links for email (linktr.ee, bio.link, etc.)
    """
    from devpost_scraper.scraper_devpost import get_profile_external_links

    result: dict[str, Any] = {
        "profile_url": profile_url,
        "external_links_walked": [],
        "github_url": "",
        "linkedin_url": "",
        "email": "",
    }

    profile_data = await get_profile_external_links(profile_url)
    all_emails: list[str] = list(profile_data.get("emails_on_profile", []))

    personal_links = [lnk for lnk in profile_data.get("external_links", []) if _is_personal_link(lnk)]

    # First pass: capture GitHub + LinkedIn URLs
    for link in personal_links:
        parsed = urlparse(link)
        domain = parsed.netloc.lstrip("www.")
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]

        if domain == "github.com" and path_parts and not result["github_url"]:
            if path_parts[0] not in _GITHUB_ORG_PATHS and len(path_parts) == 1:
                result["github_url"] = link

        if domain == "linkedin.com" and "/in/" in parsed.path and not result["linkedin_url"]:
            if "/company/" not in parsed.path:
                result["linkedin_url"] = link

    # Try GitHub API for public email
    if result["github_url"] and not all_emails:
        email = await get_github_email(result["github_url"])
        if email:
            all_emails.append(email)

    # Walk remaining external links for email
    if not all_emails:
        for link in personal_links:
            parsed = urlparse(link)
            domain = parsed.netloc.lstrip("www.")
            if domain in ("github.com", "linkedin.com"):
                continue
            if domain not in _WALKABLE_DOMAINS:
                continue
            result["external_links_walked"].append(link)
            link_data = await extract_emails_from_url(link)
            all_emails.extend(link_data.get("emails", []))
            if all_emails:
                break

    result["email"] = all_emails[0] if all_emails else ""
    return result
