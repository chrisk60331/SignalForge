"""GitHub API helpers: token rotation, fork listing, repo search, email mining."""
from __future__ import annotations

import os
import sys
from typing import Any, Literal

import httpx
from dotenv import load_dotenv

from devpost_scraper.models import GitHubFork

_GITHUB_API_URL = "https://api.github.com/users"
_GITHUB_REPOS_API = "https://api.github.com/repos"
_GITHUB_SEARCH_API = "https://api.github.com/search/repositories"

_GITHUB_TOKEN_KEYS = ("GITHUB_TOKEN", "GITHUB_TOKEN_2")
_github_token_idx = 0  # index into _GITHUB_TOKEN_KEYS for the currently active token

_GITHUB_ORG_PATHS = {"orgs", "repos", "topics", "collections", "explore", "marketplace", "about"}
_NOREPLY_SUFFIXES = ("@users.noreply.github.com",)


def _github_headers() -> dict[str, str]:
    """Build GitHub API headers.

    Reloads .env on every call so a token updated on disk is picked up immediately.
    Uses the currently active token slot; falls back to the other slot if the active
    one is empty.
    """
    load_dotenv(override=True)
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "devpost-scraper/1.0",
    }
    for offset in range(len(_GITHUB_TOKEN_KEYS)):
        key = _GITHUB_TOKEN_KEYS[(_github_token_idx + offset) % len(_GITHUB_TOKEN_KEYS)]
        token = os.environ.get(key, "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
            break
    return headers


def _rotate_github_token() -> bool:
    """Switch to the next available GitHub token after a 403 rate-limit.

    Reloads .env first so a freshly written token is immediately visible.
    Returns True if a different token was found and activated, False otherwise.
    """
    global _github_token_idx
    load_dotenv(override=True)
    for offset in range(1, len(_GITHUB_TOKEN_KEYS)):
        next_idx = (_github_token_idx + offset) % len(_GITHUB_TOKEN_KEYS)
        token = os.environ.get(_GITHUB_TOKEN_KEYS[next_idx], "").strip()
        if token:
            old_key = _GITHUB_TOKEN_KEYS[_github_token_idx]
            new_key = _GITHUB_TOKEN_KEYS[next_idx]
            print(
                f"[github] Rate-limited on {old_key}; rotating to {new_key}",
                file=sys.stderr,
            )
            _github_token_idx = next_idx
            return True
    return False


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
                if _rotate_github_token():
                    continue  # retry this page with the new token
                detail = (
                    resp.json().get("message", resp.text)
                    if resp.headers.get("content-type", "").startswith("application/json")
                    else resp.text
                )
                raise RuntimeError(
                    f"GitHub API 403 for forks: {detail}. "
                    "Set GITHUB_TOKEN / GITHUB_TOKEN_2 in .env for higher rate limits."
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
                if _rotate_github_token():
                    continue  # retry this page with the new token
                detail = (
                    resp.json().get("message", resp.text)
                    if resp.headers.get("content-type", "").startswith("application/json")
                    else resp.text
                )
                raise RuntimeError(
                    f"GitHub API 403: {detail}. "
                    "Set GITHUB_TOKEN / GITHUB_TOKEN_2 in .env for higher rate limits."
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


def _github_username_from_url(github_url: str) -> str:
    """Extract a GitHub username from a profile URL. Returns '' for non-user URLs."""
    from urllib.parse import urlparse
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


async def _get_github_email_attempt(username: str) -> str | None:
    """
    One attempt at the three GitHub email strategies.

    Returns the email string on success, ``""`` if not found, or ``None`` if a 403
    rate-limit was hit (caller should rotate token and retry).
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Strategy 1: profile email
            resp = await client.get(
                f"{_GITHUB_API_URL}/{username}",
                headers=_github_headers(),
            )
            if resp.status_code == 403:
                return None
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
            if resp.status_code == 403:
                return None
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
                    if commit_resp.status_code == 403:
                        return None
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
            if resp.status_code == 403:
                return None
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


async def get_github_email(github_url: str) -> str:
    """
    Try three GitHub API strategies to find a user's email:
    1. /users/{user} — public profile email field (often private)
    2. /users/{user}/repos?sort=pushed → /repos/{owner}/{repo}/commits — mine commit author email
    3. /users/{user}/events/public — fallback: PushEvent commit payloads

    On a 403 rate-limit, rotates to the next available token (GITHUB_TOKEN →
    GITHUB_TOKEN_2) and retries automatically.
    """
    username = _github_username_from_url(github_url)
    if not username:
        return ""

    for _attempt in range(len(_GITHUB_TOKEN_KEYS)):
        result = await _get_github_email_attempt(username)
        if result is not None:
            return result
        if not _rotate_github_token():
            break

    return ""
