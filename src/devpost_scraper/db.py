from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from devpost_scraper.models import Hackathon, HackathonParticipant

_DEFAULT_DB = "devpost_harvest.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS hackathons (
    id            INTEGER PRIMARY KEY,
    url           TEXT UNIQUE NOT NULL,
    title         TEXT,
    organization_name TEXT,
    open_state    TEXT,
    submission_period_dates TEXT,
    registrations_count INTEGER,
    prize_amount  TEXT,
    themes        TEXT,
    invite_only   INTEGER,
    first_seen_at TEXT NOT NULL,
    last_scraped_at TEXT
);

CREATE TABLE IF NOT EXISTS participants (
    hackathon_url TEXT NOT NULL,
    hackathon_title TEXT,
    username      TEXT NOT NULL,
    name          TEXT,
    specialty     TEXT,
    profile_url   TEXT,
    github_url    TEXT,
    linkedin_url  TEXT,
    email         TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    event_emitted_at TEXT,
    PRIMARY KEY (hackathon_url, username)
);
"""


class HarvestDB:
    def __init__(self, db_path: str = _DEFAULT_DB) -> None:
        self._path = Path(db_path)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(participants)").fetchall()}
        if "hackathon_title" not in cols:
            self._conn.execute("ALTER TABLE participants ADD COLUMN hackathon_title TEXT")
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def upsert_hackathon(self, h: Hackathon) -> None:
        now = _now_iso()
        self._conn.execute(
            """INSERT INTO hackathons
                   (id, url, title, organization_name, open_state,
                    submission_period_dates, registrations_count, prize_amount,
                    themes, invite_only, first_seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   title=excluded.title,
                   organization_name=excluded.organization_name,
                   open_state=excluded.open_state,
                   submission_period_dates=excluded.submission_period_dates,
                   registrations_count=excluded.registrations_count,
                   prize_amount=excluded.prize_amount,
                   themes=excluded.themes,
                   invite_only=excluded.invite_only
            """,
            (
                h.id, h.url, h.title, h.organization_name, h.open_state,
                h.submission_period_dates, h.registrations_count, h.prize_amount,
                h.themes, int(h.invite_only), now,
            ),
        )
        self._conn.commit()

    def upsert_participants(
        self, participants: list[HackathonParticipant],
    ) -> list[HackathonParticipant]:
        """Insert or update participants. Returns only the NEW ones (not previously seen)."""
        now = _now_iso()
        new: list[HackathonParticipant] = []

        for p in participants:
            existing = self._conn.execute(
                "SELECT 1 FROM participants WHERE hackathon_url=? AND username=?",
                (p.hackathon_url, p.username),
            ).fetchone()

            if existing is None:
                new.append(p)
                self._conn.execute(
                    """INSERT INTO participants
                           (hackathon_url, hackathon_title, username, name, specialty,
                            profile_url, github_url, linkedin_url, email,
                            first_seen_at, last_seen_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        p.hackathon_url, p.hackathon_title, p.username, p.name,
                        p.specialty, p.profile_url, p.github_url, p.linkedin_url,
                        p.email, now, now,
                    ),
                )
            else:
                self._conn.execute(
                    """UPDATE participants
                       SET hackathon_title=?, name=?, specialty=?, profile_url=?,
                           github_url=?, linkedin_url=?, email=?, last_seen_at=?
                       WHERE hackathon_url=? AND username=?
                    """,
                    (
                        p.hackathon_title, p.name, p.specialty, p.profile_url,
                        p.github_url, p.linkedin_url, p.email, now,
                        p.hackathon_url, p.username,
                    ),
                )

        self._conn.commit()
        return new

    def mark_event_emitted(self, hackathon_url: str, username: str) -> None:
        self._conn.execute(
            "UPDATE participants SET event_emitted_at=? WHERE hackathon_url=? AND username=?",
            (_now_iso(), hackathon_url, username),
        )
        self._conn.commit()

    def get_unemitted_participants(self, hackathon_url: str) -> list[HackathonParticipant]:
        """Return participants that have an email but haven't had events emitted yet."""
        rows = self._conn.execute(
            """SELECT * FROM participants
               WHERE hackathon_url=? AND email != '' AND event_emitted_at IS NULL""",
            (hackathon_url,),
        ).fetchall()
        return [
            HackathonParticipant(
                hackathon_url=r["hackathon_url"],
                hackathon_title=r["hackathon_title"] or "",
                username=r["username"],
                name=r["name"] or "",
                specialty=r["specialty"] or "",
                profile_url=r["profile_url"] or "",
                github_url=r["github_url"] or "",
                linkedin_url=r["linkedin_url"] or "",
                email=r["email"] or "",
            )
            for r in rows
        ]

    def hackathon_scraped(self, hackathon_url: str) -> bool:
        row = self._conn.execute(
            "SELECT last_scraped_at FROM hackathons WHERE url=?",
            (hackathon_url,),
        ).fetchone()
        return row is not None and row["last_scraped_at"] is not None

    def mark_hackathon_scraped(self, hackathon_url: str) -> None:
        self._conn.execute(
            "UPDATE hackathons SET last_scraped_at=? WHERE url=?",
            (_now_iso(), hackathon_url),
        )
        self._conn.commit()

    def stats(self) -> dict[str, int]:
        hcount = self._conn.execute("SELECT COUNT(*) FROM hackathons").fetchone()[0]
        pcount = self._conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
        emitted = self._conn.execute(
            "SELECT COUNT(*) FROM participants WHERE event_emitted_at IS NOT NULL"
        ).fetchone()[0]
        with_email = self._conn.execute(
            "SELECT COUNT(*) FROM participants WHERE email != ''"
        ).fetchone()[0]
        return {
            "hackathons": hcount,
            "participants": pcount,
            "with_email": with_email,
            "events_emitted": emitted,
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
