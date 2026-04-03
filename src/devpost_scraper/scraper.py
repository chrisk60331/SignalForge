from __future__ import annotations

import os
import re
import sys
from typing import Any, Literal
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from devpost_scraper.models import GitHubFork

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

_SEARCH_URL = "https://devpost.com/software/search"
_HACKATHONS_API_URL = "https://devpost.com/api/hackathons"
_GITHUB_API_URL = "https://api.github.com/users"
_GITHUB_REPOS_API = "https://api.github.com/repos"


def _github_headers() -> dict[str, str]:
    """Build GitHub API headers, including auth token if GITHUB_TOKEN is set."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "devpost-scraper/1.0",
    }
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fork_from_repo_json(data: dict[str, Any]) -> GitHubFork | None:
    """Build ``GitHubFork`` from a repo object in ``/repos/.../forks`` response."""
    owner = data.get("owner") or {}
    login = (owner.get("login") or "").strip()
    full_name = (data.get("full_name") or "").strip()
    if not login or not full_name:
        return None
    return GitHubFork(
        full_name=full_name,
        owner_login=login,
        owner_html_url=(owner.get("html_url") or "").strip(),
        pushed_at=(data.get("pushed_at") or "").strip(),
        html_url=(data.get("html_url") or "").strip(),
    )


async def fetch_repo_forks(
    owner: str,
    repo: str,
    *,
    max_forks: int,
    mode: Literal["top_by_pushed", "first_n"],
    progress: bool = False,
) -> list[GitHubFork]:
    """
    Paginate ``GET /repos/{owner}/{repo}/forks`` (``per_page=100``).

    * ``top_by_pushed``: load every fork page, sort by ``pushed_at`` descending,
      return the top ``max_forks``.
    * ``first_n``: stop after collecting ``max_forks`` forks (API order: ``sort=newest``).
    """
    url = f"{_GITHUB_REPOS_API}/{owner}/{repo}/forks"
    collected: list[GitHubFork] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        page = 1
        while True:
            resp = await client.get(
                url,
                params={"per_page": 100, "page": page, "sort": "newest"},
                headers=_github_headers(),
            )
            if resp.status_code == 403:
                detail = resp.json().get("message", resp.text) if resp.headers.get(
                    "content-type", ""
                ).startswith("application/json") else resp.text
                raise RuntimeError(
                    f"GitHub API 403 for forks: {detail}. "
                    "Set GITHUB_TOKEN in .env for higher rate limits."
                )
            resp.raise_for_status()
            batch: list[Any] = resp.json()
            if not batch:
                break

            for raw in batch:
                fk = fork_from_repo_json(raw if isinstance(raw, dict) else {})
                if fk:
                    collected.append(fk)
                    if mode == "first_n" and len(collected) >= max_forks:
                        return collected[:max_forks]

            if progress and page % 5 == 0:
                print(
                    f"[github-forks] page {page} scanned ({len(collected)} forks so far)…",
                    file=sys.stderr,
                )

            if len(batch) < 100:
                break
            page += 1

    if mode == "top_by_pushed":
        collected.sort(key=lambda f: f.pushed_at or "", reverse=True)
        collected = collected[:max_forks]

    return collected


_GITHUB_SEARCH_API = "https://api.github.com/search/repositories"


async def search_github_repos(
    query: str,
    *,
    max_results: int = 100,
    sort: str = "stars",
) -> list[dict[str, Any]]:
    """Search GitHub repositories using the /search/repositories API.

    Returns a list of dicts with keys:
      full_name, owner_login, owner_html_url, owner_type,
      description, stars, html_url, topics
    """
    collected: list[dict[str, Any]] = []
    per_page = min(100, max_results)
    page = 1

    async with httpx.AsyncClient(timeout=30.0) as client:
        while len(collected) < max_results:
            resp = await client.get(
                _GITHUB_SEARCH_API,
                params={
                    "q": query,
                    "sort": sort,
                    "order": "desc",
                    "per_page": per_page,
                    "page": page,
                },
                headers=_github_headers(),
            )
            if resp.status_code == 403:
                detail = (
                    resp.json().get("message", resp.text)
                    if resp.headers.get("content-type", "").startswith("application/json")
                    else resp.text
                )
                raise RuntimeError(
                    f"GitHub API 403: {detail}. "
                    "Set GITHUB_TOKEN in .env for higher rate limits."
                )
            if resp.status_code == 422:
                detail = resp.json().get("message", resp.text)
                raise RuntimeError(f"GitHub search API 422: {detail}")
            resp.raise_for_status()

            data = resp.json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                owner = item.get("owner") or {}
                collected.append(
                    {
                        "full_name": (item.get("full_name") or "").strip(),
                        "owner_login": (owner.get("login") or "").strip(),
                        "owner_html_url": (owner.get("html_url") or "").strip(),
                        "owner_type": (owner.get("type") or "").strip(),
                        "description": (item.get("description") or "").strip(),
                        "stars": item.get("stargazers_count", 0),
                        "html_url": (item.get("html_url") or "").strip(),
                        "topics": ", ".join(item.get("topics") or []),
                    }
                )
                if len(collected) >= max_results:
                    return collected

            if len(items) < per_page:
                break
            page += 1

    return collected


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


def _text(el: Any) -> str:
    if el is None:
        return ""
    return el.get_text(strip=True)


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


_DEVPOST_NON_PROFILE_PATHS = {
    "software", "hackathons", "settings", "portfolio", "search",
    "about", "contact", "help", "careers", "login", "register",
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


async def get_hackathon_participants(
    hackathon_url: str,
    jwt_token: str,
    page: int = 1,
) -> dict[str, Any]:
    """
    Fetch one page of participants from a Devpost hackathon participants page.
    Requires a valid Devpost session JWT (sent as _devpost_session cookie).
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

    # Each participant is a div.participant with data-participant-id
    cards = soup.select("div.participant")

    # CSS classes on participant card encode specialty, e.g. "participant full-stack-developer"
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

        # Name lives in img[alt] or h5 inside .user-name
        img = card.select_one("img[alt]")
        name = img["alt"].strip() if img and img.get("alt") else slug

        # Specialty is encoded as extra CSS class on the card div
        card_classes = [c for c in card.get("class", []) if c not in _CARD_SKIP_CLASSES]
        specialty = card_classes[0].replace("-", " ").title() if card_classes else ""

        participants.append({
            "username": slug,
            "name": name,
            "profile_url": profile_href,
            "specialty": specialty,
        })

    # Pagination: Devpost renders <a rel="next"> when more pages exist
    next_link = soup.select_one('a[rel="next"]')
    has_more = next_link is not None

    return {"participants": participants, "has_more": has_more, "page": page}


_GITHUB_ORG_PATHS = {"orgs", "repos", "topics", "collections", "explore", "marketplace", "about"}


_NOREPLY_SUFFIXES = ("@users.noreply.github.com",)


def _github_username_from_url(github_url: str) -> str:
    """Extract a GitHub username from a profile URL. Returns '' for non-user URLs."""
    parsed = urlparse(github_url)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(path_parts) != 1 or path_parts[0] in _GITHUB_ORG_PATHS:
        return ""
    return path_parts[0]


def _is_real_email(email: str) -> bool:
    """Filter out GitHub noreply and placeholder addresses."""
    if not email:
        return False
    email = email.lower().strip()
    if email.endswith(_NOREPLY_SUFFIXES):
        return False
    if "noreply" in email or "github.com" in email:
        return False
    return "." in email.split("@")[-1]


async def get_github_email(github_url: str) -> str:
    """
    Try three GitHub API strategies to find a user's email:
    1. /users/{user} — public profile email field (often private)
    2. /users/{user}/repos?sort=pushed → /repos/{owner}/{repo}/commits — mine commit author email
    3. /users/{user}/events/public — fallback: PushEvent commit payloads
    """
    username = _github_username_from_url(github_url)
    if not username:
        return ""

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Strategy 1: profile email
            resp = await client.get(
                f"{_GITHUB_API_URL}/{username}",
                headers=_github_headers(),
            )
            if resp.status_code == 200:
                email = (resp.json().get("email") or "").strip()
                if _is_real_email(email):
                    return email

            # Strategy 2: most-recently-pushed repo → commits → author email
            resp = await client.get(
                f"{_GITHUB_API_URL}/{username}/repos",
                params={"sort": "pushed", "per_page": 3, "type": "owner"},
                headers=_github_headers(),
            )
            if resp.status_code == 200:
                for repo in resp.json():
                    full_name = repo.get("full_name", "")
                    if repo.get("fork") or not full_name:
                        continue
                    commit_resp = await client.get(
                        f"https://api.github.com/repos/{full_name}/commits",
                        params={"author": username, "per_page": 5},
                        headers=_github_headers(),
                    )
                    if commit_resp.status_code != 200:
                        continue
                    for c in commit_resp.json():
                        author = c.get("commit", {}).get("author", {})
                        email = (author.get("email") or "").strip().lower()
                        if _is_real_email(email):
                            return email

            # Strategy 3: PushEvent payloads (fallback, often has 0 commits)
            resp = await client.get(
                f"{_GITHUB_API_URL}/{username}/events/public",
                params={"per_page": 20},
                headers=_github_headers(),
            )
            if resp.status_code == 200:
                for event in resp.json():
                    if event.get("type") != "PushEvent":
                        continue
                    for commit in event.get("payload", {}).get("commits", []):
                        email = (commit.get("author", {}).get("email") or "").strip().lower()
                        if _is_real_email(email):
                            return email

    except Exception:
        pass

    return ""


_DEVPOST_OWNED_DOMAINS = {
    "devpost.com", "devpost.team", "info.devpost.com",
    "secure.devpost.com", "d2dmyh35ffsxbl.cloudfront.net",
    "d112y698adiu2z.cloudfront.net",
}


def _is_personal_link(url: str) -> bool:
    """Filter out Devpost-owned links (nav, footer, CDN) from external link lists."""
    parsed = urlparse(url)
    domain = parsed.netloc.lstrip("www.")
    return domain not in _DEVPOST_OWNED_DOMAINS


async def find_participant_email(profile_url: str) -> dict[str, Any]:
    """
    Enrich a participant from their Devpost profile:
    1. Extract GitHub URL, LinkedIn URL from profile social links
    2. Try GitHub API for public email
    3. Walk other external links for email (linktr.ee, bio.link, etc.)
    """
    result: dict[str, Any] = {
        "profile_url": profile_url,
        "external_links_walked": [],
        "github_url": "",
        "linkedin_url": "",
        "email": "",
    }

    profile_data = await get_profile_external_links(profile_url)
    all_emails: list[str] = list(profile_data.get("emails_on_profile", []))

    personal_links = [l for l in profile_data.get("external_links", []) if _is_personal_link(l)]

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


# ---------------------------------------------------------------------------
# RB2B export fetcher
# ---------------------------------------------------------------------------

_RB2B_EXPORTS_URL = "https://app.rb2b.com/profiles/exports"
_RB2B_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


async def fetch_rb2b_exports(
    session_cookie: str,
    uid_cookie: str,
) -> list[dict[str, Any]]:
    """
    Fetch https://app.rb2b.com/profiles/exports and return a list of export entries.

    Each entry: {filename, url, row_count, date_label, date (YYYY-MM-DD)}

    Requires both cookies:
      - _rb2b_session  (Rails session — copy from browser DevTools)
      - _reb2buid      (device/account UID — copy from browser DevTools)
    """
    cookie_header = f"_rb2b_session={session_cookie}; _reb2buid={uid_cookie}"
    headers = {
        "Cookie": cookie_header,
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": _RB2B_USER_AGENT,
        "Referer": "https://app.rb2b.com/",
    }

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(_RB2B_EXPORTS_URL, headers=headers)

    if resp.status_code == 302 or "Login with a Magic Link" in resp.text:
        raise PermissionError(
            "RB2B session is invalid or expired. "
            "Copy fresh _rb2b_session and _reb2buid cookie values from your browser "
            "DevTools → Application → Cookies → app.rb2b.com "
            "and update RB2B_SESSION / REB2B_UID in .env"
        )

    soup = BeautifulSoup(resp.text, "html.parser")

    exports: list[dict[str, Any]] = []
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        link_tag = cells[0].find("a", href=True)
        if not link_tag:
            continue
        filename = link_tag.get_text(strip=True)
        url = link_tag["href"]
        row_count_text = cells[1].get_text(strip=True)
        date_text = cells[2].get_text(strip=True)  # e.g. "03/31/2026 08:20:27 PM EDT"

        row_count = int(row_count_text) if row_count_text.isdigit() else 0

        # Parse date portion (MM/DD/YYYY) → YYYY-MM-DD
        date_iso = ""
        date_match = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_text)
        if date_match:
            mm, dd, yyyy = date_match.groups()
            date_iso = f"{yyyy}-{mm}-{dd}"

        exports.append(
            {
                "filename": filename,
                "url": url,
                "row_count": row_count,
                "date_label": date_text,
                "date": date_iso,
            }
        )

    return exports


async def download_rb2b_export(url: str, dest_path: str) -> None:
    """Download a pre-signed S3 URL to dest_path."""
    import pathlib
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(url)
    resp.raise_for_status()
    pathlib.Path(dest_path).write_bytes(resp.content)


async def find_author_email(project_url: str) -> dict[str, Any]:
    """
    Full chain: project page → author profile(s) → external links → emails.
    Returns the first email found along with the chain of URLs walked.
    """
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
