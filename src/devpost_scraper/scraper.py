"""Backward-compatible re-export facade — all public scraper names in one place.

The actual implementations live in the sub-modules:
  scraper_github   — GitHub API: token rotation, forks, repo search, email mining
  scraper_devpost  — Devpost: search, hackathon listing, project details, participants
  scraper_email    — Email walking: extract, find_author_email, find_participant_email
  scraper_hn       — Hacker News Show HN scraper
  scraper_rb2b     — RB2B export fetcher
  scraper_devto    — dev.to challenge scraper
"""
from devpost_scraper.scraper_github import (
    fetch_repo_forks,
    fork_from_repo_json,
    get_github_email,
    search_github_repos,
)
from devpost_scraper.scraper_devpost import (
    get_author_profile_urls,
    get_hackathon_participants,
    get_profile_external_links,
    get_project_details,
    list_hackathons,
    search_projects,
)
from devpost_scraper.scraper_email import (
    extract_emails_from_url,
    find_author_email,
    find_participant_email,
)
from devpost_scraper.scraper_hn import list_hn_show_posts
from devpost_scraper.scraper_rb2b import download_rb2b_export, fetch_rb2b_exports
from devpost_scraper.scraper_devto import (
    get_devto_challenge_tag,
    get_devto_tag_articles,
    list_devto_challenges,
)

__all__ = [
    # github
    "fetch_repo_forks",
    "fork_from_repo_json",
    "get_github_email",
    "search_github_repos",
    # devpost
    "get_author_profile_urls",
    "get_hackathon_participants",
    "get_profile_external_links",
    "get_project_details",
    "list_hackathons",
    "search_projects",
    # email
    "extract_emails_from_url",
    "find_author_email",
    "find_participant_email",
    # hn
    "list_hn_show_posts",
    # rb2b
    "download_rb2b_export",
    "fetch_rb2b_exports",
    # devto
    "get_devto_challenge_tag",
    "get_devto_tag_articles",
    "list_devto_challenges",
]
