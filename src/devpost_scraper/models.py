from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class HackathonParticipant(BaseModel):
    model_config = ConfigDict(extra="ignore")

    hackathon_url: str = ""
    username: str = ""
    name: str = ""
    specialty: str = ""
    profile_url: str = ""
    github_url: str = ""
    linkedin_url: str = ""
    email: str = ""

    @classmethod
    def fieldnames(cls) -> list[str]:
        return ["hackathon_url", "username", "name", "specialty", "profile_url", "github_url", "linkedin_url", "email"]


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
