"""Hacker News Show HN scraper — finds GitHub-linked posts."""
from __future__ import annotations

import sys
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from devpost_scraper.scraper_devpost import _HTML_HEADERS

_HN_SHOW_URL = "https://news.ycombinator.com/show"


async def list_hn_show_posts(pages: int = 1) -> list[dict[str, Any]]:
    """Scrape Hacker News Show HN and return posts that link to GitHub.

    Paginates up to ``pages`` pages.  De-duplicates by GitHub repo owner so each
    owner appears at most once.

    Each returned dict:
        title        — Show HN post title
        github_url   — GitHub *profile* URL (https://github.com/{owner})
        post_url     — original GitHub repo/org URL from the post
        hn_username  — HN submitter username
        hn_profile_url — https://news.ycombinator.com/user?id={username}
    """
    posts: list[dict[str, Any]] = []
    seen_owners: set[str] = set()
    url = _HN_SHOW_URL

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for page_idx in range(pages):
            page_num = page_idx + 1
            resp = await client.get(url, headers=_HTML_HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            page_github = 0
            rows = soup.select("tr.athing")
            for row in rows:
                title_span = row.select_one("span.titleline > a")
                if not title_span:
                    continue
                href: str = title_span.get("href", "")
                title: str = title_span.get_text(strip=True)

                if not href.startswith("http"):
                    continue

                parsed = urlparse(href)
                domain = parsed.netloc.lstrip("www.")
                if domain != "github.com":
                    continue

                # Subtext row immediately follows the athing row
                subtext_row = row.find_next_sibling("tr")
                if not subtext_row:
                    continue
                hn_user_tag = subtext_row.select_one("a.hnuser")
                if not hn_user_tag:
                    continue
                hn_username: str = hn_user_tag.get_text(strip=True)

                # GitHub owner is the first path segment
                path_parts = [p for p in parsed.path.strip("/").split("/") if p]
                if not path_parts:
                    continue
                github_owner = path_parts[0]

                if github_owner in seen_owners:
                    continue
                seen_owners.add(github_owner)
                page_github += 1

                posts.append({
                    "title": title,
                    "github_url": f"https://github.com/{github_owner}",
                    "post_url": href,
                    "hn_username": hn_username,
                    "hn_profile_url": f"https://news.ycombinator.com/user?id={hn_username}",
                })

            next_link = soup.select_one("a.morelink")
            next_href: str = next_link.get("href", "") if next_link else ""
            next_url = (
                f"https://news.ycombinator.com/{next_href}"
                if next_href and not next_href.startswith("http")
                else next_href
            )
            print(
                f"  [hn] page {page_num}: {len(rows)} posts, "
                f"{page_github} new GitHub → {url}",
                file=sys.stderr,
            )

            if page_idx >= pages - 1 or not next_link:
                if not next_link:
                    print(
                        f"  [hn] no more pages after page {page_num}",
                        file=sys.stderr,
                    )
                break
            url = next_url

    return posts
