from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class Hackathon(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    url: str
    title: str = ""
    organization_name: str = ""
    open_state: str = ""
    submission_period_dates: str = ""
    registrations_count: int = 0
    prize_amount: str = ""
    themes: str = ""
    invite_only: bool = False


class HackathonParticipant(BaseModel):
    model_config = ConfigDict(extra="ignore")

    hackathon_url: str = ""
    hackathon_title: str = ""
    username: str = ""
    name: str = ""
    specialty: str = ""
    profile_url: str = ""
    github_url: str = ""
    linkedin_url: str = ""
    email: str = ""

    @classmethod
    def fieldnames(cls) -> list[str]:
        return ["hackathon_url", "hackathon_title", "username", "name", "specialty", "profile_url", "github_url", "linkedin_url", "email"]


class DevpostHackathonEvent(BaseModel):
    """Payload for the Customer.io ``devpost_hackathon`` event."""

    hackathon_url: str
    hackathon_title: str
    username: str
    name: str
    specialty: str
    profile_url: str
    github_url: str
    linkedin_url: str


class GitHubFork(BaseModel):
    """One fork from ``GET /repos/{owner}/{repo}/forks`` (subset of repo object)."""

    model_config = ConfigDict(extra="ignore")

    full_name: str
    owner_login: str
    owner_html_url: str = ""
    pushed_at: str = ""
    html_url: str = ""


class Rb2bVisitor(BaseModel):
    """One row from an RB2B daily export CSV."""

    model_config = ConfigDict(extra="ignore")

    visitor_id: str = ""          # email if identified, else "anon:<linkedin|company>"
    email: str = ""
    first_name: str = ""
    last_name: str = ""
    linkedin_url: str = ""
    company_name: str = ""
    title: str = ""
    industry: str = ""
    employee_count: str = ""
    estimated_revenue: str = ""
    city: str = ""
    state: str = ""
    website: str = ""
    rb2b_last_seen_at: str = ""
    rb2b_first_seen_at: str = ""
    most_recent_referrer: str = ""
    recent_page_count: str = ""
    recent_page_urls: str = ""    # raw JSON string from CSV
    profile_type: str = ""
    source_file: str = ""

    @classmethod
    def from_csv_row(cls, row: dict, source_file: str = "") -> "Rb2bVisitor":
        import json as _json
        email = row.get("WorkEmail", "").strip()
        linkedin_url = row.get("LinkedInUrl", "").strip()
        company_name = row.get("CompanyName", "").strip()
        visitor_id = email or f"anon:{linkedin_url or company_name or 'unknown'}"
        raw_urls = row.get("RecentPageUrls", "").strip()
        try:
            page_urls = _json.dumps(_json.loads(raw_urls)) if raw_urls else ""
        except (ValueError, TypeError):
            page_urls = ""
        return cls(
            visitor_id=visitor_id,
            email=email,
            first_name=row.get("FirstName", "").strip(),
            last_name=row.get("LastName", "").strip(),
            linkedin_url=linkedin_url,
            company_name=company_name,
            title=row.get("Title", "").strip(),
            industry=row.get("Industry", "").strip(),
            employee_count=row.get("EstimatedEmployeeCount", "").strip(),
            estimated_revenue=row.get("EstimateRevenue", "").strip(),
            city=row.get("City", "").strip(),
            state=row.get("State", "").strip(),
            website=row.get("Website", "").strip(),
            rb2b_last_seen_at=row.get("LastSeenAt", "").strip(),
            rb2b_first_seen_at=row.get("FirstSeenAt", "").strip(),
            most_recent_referrer=row.get("MostRecentReferrer", "").strip(),
            recent_page_count=row.get("RecentPageCount", "").strip(),
            recent_page_urls=page_urls,
            profile_type=row.get("ProfileType", "").strip(),
            source_file=source_file,
        )


class DevpostProject(BaseModel):
    model_config = ConfigDict(extra="ignore")

    search_term: str = ""
    title: str = ""
    tagline: str = ""
    url: str = ""
    hackathon_name: str = ""
    hackathon_url: str = ""
    summary: str = ""
    built_with: str = ""
    prizes: str = ""
    team_size: str = ""
    author_profile_url: str = ""
    email: str = ""

    @classmethod
    def fieldnames(cls) -> list[str]:
        return [
            "search_term",
            "title",
            "tagline",
            "url",
            "hackathon_name",
            "hackathon_url",
            "summary",
            "built_with",
            "prizes",
            "team_size",
            "author_profile_url",
            "email",
        ]
