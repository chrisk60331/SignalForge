"""RB2B export fetcher — list and download visitor export CSVs."""
from __future__ import annotations

import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

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
