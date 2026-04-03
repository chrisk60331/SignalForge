from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from devpost_scraper.models import Hackathon, HackathonParticipant, Rb2bVisitor

_DEFAULT_DB = "devpost_harvest.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    command     TEXT NOT NULL,
    args        TEXT NOT NULL DEFAULT '[]',
    pid         INTEGER,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    exit_code   INTEGER,
    status      TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS devto_challenges (
    tag             TEXT PRIMARY KEY,
    title           TEXT,
    challenge_url   TEXT,
    state           TEXT,
    first_seen_at   TEXT NOT NULL,
    last_scraped_at TEXT
);

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

CREATE TABLE IF NOT EXISTS rb2b_visitors (
    visitor_id        TEXT PRIMARY KEY,
    email             TEXT,
    first_name        TEXT,
    last_name         TEXT,
    linkedin_url      TEXT,
    company_name      TEXT,
    title             TEXT,
    industry          TEXT,
    employee_count    TEXT,
    estimated_revenue TEXT,
    city              TEXT,
    state             TEXT,
    website           TEXT,
    rb2b_last_seen_at TEXT,
    rb2b_first_seen_at TEXT,
    most_recent_referrer TEXT,
    recent_page_count TEXT,
    recent_page_urls  TEXT,
    profile_type      TEXT,
    source_file       TEXT,
    imported_at       TEXT NOT NULL,
    event_emitted_at  TEXT
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
        """Insert or update participants. Returns only the NEW ones (not previously seen).

        Email-level dedup: if a participant's email already exists in the DB (from any
        hackathon), they are skipped entirely — the existing row keeps its event_emitted_at
        flag and won't trigger duplicate Customer.io events.
        """
        now = _now_iso()
        new: list[HackathonParticipant] = []

        for p in participants:
            row = self._conn.execute(
                "SELECT email FROM participants WHERE hackathon_url=? AND username=?",
                (p.hackathon_url, p.username),
            ).fetchone()

            if row is None:
                # Email-level dedup: skip insert if this email is already in the DB.
                if p.email and p.email.strip():
                    email_row = self._conn.execute(
                        "SELECT 1 FROM participants WHERE LOWER(email)=? LIMIT 1",
                        (p.email.strip().lower(),),
                    ).fetchone()
                    if email_row is not None:
                        continue

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
                prev_email = row["email"] or ""
                merged_email = p.email.strip() if (p.email and p.email.strip()) else prev_email
                self._conn.execute(
                    """UPDATE participants
                       SET hackathon_title=?, name=?, specialty=?, profile_url=?,
                           github_url=?, linkedin_url=?, email=?, last_seen_at=?
                       WHERE hackathon_url=? AND username=?
                    """,
                    (
                        p.hackathon_title, p.name, p.specialty, p.profile_url,
                        p.github_url, p.linkedin_url, merged_email, now,
                        p.hackathon_url, p.username,
                    ),
                )

        self._conn.commit()
        return new

    def _dedup_safe_email(self, email: str, hackathon_url: str, username: str) -> str:
        """Return email only if no OTHER row already owns it; empty string otherwise."""
        if not email or not email.strip():
            return email
        conflict = self._conn.execute(
            """SELECT 1 FROM participants
               WHERE LOWER(email)=?
                 AND NOT (hackathon_url=? AND username=?)
               LIMIT 1""",
            (email.strip().lower(), hackathon_url, username),
        ).fetchone()
        return "" if conflict else email

    def update_participant_enrichment(self, p: HackathonParticipant) -> None:
        """Update email/github/linkedin fields for a single participant (commits immediately).

        Email is suppressed if another row already owns it, preventing duplicate events.
        """
        safe_email = self._dedup_safe_email(p.email, p.hackathon_url, p.username)
        self._conn.execute(
            """UPDATE participants
               SET email=?, github_url=?, linkedin_url=?, last_seen_at=?
               WHERE hackathon_url=? AND username=?""",
            (safe_email, p.github_url, p.linkedin_url, _now_iso(), p.hackathon_url, p.username),
        )
        self._conn.commit()

    def update_participant_enrichment_batch(self, participants: list[HackathonParticipant]) -> None:
        """Bulk-update email/github/linkedin for multiple participants in one commit.

        Each email is suppressed if another row already owns it, preventing duplicate events.
        """
        now = _now_iso()
        for p in participants:
            safe_email = self._dedup_safe_email(p.email, p.hackathon_url, p.username)
            self._conn.execute(
                """UPDATE participants
                   SET email=?, github_url=?, linkedin_url=?, last_seen_at=?
                   WHERE hackathon_url=? AND username=?""",
                (safe_email, p.github_url, p.linkedin_url, now, p.hackathon_url, p.username),
            )
        self._conn.commit()

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

    def all_unemitted_participants(self) -> list[HackathonParticipant]:
        """Return Devpost-only participants with email but no event emitted (excludes GitHub forks, dev.to, and HN)."""
        rows = self._conn.execute(
            "SELECT * FROM participants "
            "WHERE email != '' AND event_emitted_at IS NULL "
            "AND hackathon_url NOT LIKE 'github:forks:%' "
            "AND hackathon_url NOT LIKE 'github:search:%' "
            "AND hackathon_url NOT LIKE 'devto:challenge:%' "
            "AND hackathon_url NOT LIKE 'hn:show%'"
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

    def all_unemitted_fork_participants(self) -> list[HackathonParticipant]:
        """Return GitHub fork owners with email but no event emitted, grouped by source repo."""
        rows = self._conn.execute(
            "SELECT * FROM participants "
            "WHERE email != '' AND event_emitted_at IS NULL "
            "AND hackathon_url LIKE 'github:forks:%' "
            "ORDER BY hackathon_url"
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

    def all_unemitted_search_participants(self) -> list[HackathonParticipant]:
        """Return GitHub search repo owners with email but no event emitted, grouped by query."""
        rows = self._conn.execute(
            "SELECT * FROM participants "
            "WHERE email != '' AND event_emitted_at IS NULL "
            "AND hackathon_url LIKE 'github:search:%' "
            "ORDER BY hackathon_url"
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

    def get_participants_with_linkedin_no_email(self) -> list[HackathonParticipant]:
        """Return all participants that have a linkedin_url but no email, across all sources."""
        rows = self._conn.execute(
            """SELECT * FROM participants
               WHERE (linkedin_url IS NOT NULL AND linkedin_url != '')
                 AND (email IS NULL OR email = '')
               ORDER BY hackathon_url, username"""
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
                email="",
            )
            for r in rows
        ]

    def get_participants_without_email(self, limit: int = 0) -> list[HackathonParticipant]:
        """Return participants that have a profile_url but no email yet."""
        sql = (
            "SELECT * FROM participants "
            "WHERE (email IS NULL OR email = '') AND (profile_url IS NOT NULL AND profile_url != '')"
            " AND event_emitted_at IS NULL"
            " ORDER BY first_seen_at ASC"
        )
        if limit > 0:
            sql += f" LIMIT {limit}"
        rows = self._conn.execute(sql).fetchall()
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
                email="",
            )
            for r in rows
        ]

    def get_hackathon_meta(self, urls: list[str]) -> dict[str, dict]:
        """Return a mapping of hackathon_url → {submission_period_dates, open_state} for the given URLs."""
        if not urls:
            return {}
        placeholders = ",".join("?" * len(urls))
        rows = self._conn.execute(
            f"SELECT url, submission_period_dates, open_state FROM hackathons WHERE url IN ({placeholders})",
            urls,
        ).fetchall()
        return {
            r["url"]: {
                "submission_period_dates": r["submission_period_dates"] or "",
                "open_state": r["open_state"] or "",
            }
            for r in rows
        }

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

    # ------------------------------------------------------------------
    # dev.to challenges
    # ------------------------------------------------------------------

    def upsert_devto_challenge(
        self, tag: str, title: str, challenge_url: str, state: str
    ) -> None:
        now = _now_iso()
        self._conn.execute(
            """INSERT INTO devto_challenges (tag, title, challenge_url, state, first_seen_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(tag) DO UPDATE SET
                   title=excluded.title,
                   challenge_url=excluded.challenge_url,
                   state=excluded.state
            """,
            (tag, title, challenge_url, state, now),
        )
        self._conn.commit()

    def devto_challenge_scraped(self, tag: str) -> bool:
        row = self._conn.execute(
            "SELECT last_scraped_at FROM devto_challenges WHERE tag=?",
            (tag,),
        ).fetchone()
        return row is not None and row["last_scraped_at"] is not None

    def mark_devto_challenge_scraped(self, tag: str) -> None:
        self._conn.execute(
            "UPDATE devto_challenges SET last_scraped_at=? WHERE tag=?",
            (_now_iso(), tag),
        )
        self._conn.commit()

    def all_unemitted_devto_participants(self) -> list[HackathonParticipant]:
        """Return dev.to challenge submitters with email but no event emitted."""
        rows = self._conn.execute(
            "SELECT * FROM participants "
            "WHERE email != '' AND event_emitted_at IS NULL "
            "AND hackathon_url LIKE 'devto:challenge:%' "
            "ORDER BY hackathon_url"
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

    def all_unemitted_hn_participants(self) -> list[HackathonParticipant]:
        """Return Hacker News Show HN posters with email but no event emitted."""
        rows = self._conn.execute(
            "SELECT * FROM participants "
            "WHERE email != '' AND event_emitted_at IS NULL "
            "AND hackathon_url LIKE 'hn:show%' "
            "ORDER BY hackathon_url"
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

    # ------------------------------------------------------------------
    # RB2B visitors
    # ------------------------------------------------------------------

    def upsert_rb2b_visitors(self, visitors: list[Rb2bVisitor]) -> list[Rb2bVisitor]:
        """Upsert RB2B visitors. Returns only the NEW ones."""
        now = _now_iso()
        new: list[Rb2bVisitor] = []
        for v in visitors:
            row = self._conn.execute(
                "SELECT visitor_id FROM rb2b_visitors WHERE visitor_id=?",
                (v.visitor_id,),
            ).fetchone()
            if row is None:
                new.append(v)
                self._conn.execute(
                    """INSERT INTO rb2b_visitors
                           (visitor_id, email, first_name, last_name, linkedin_url,
                            company_name, title, industry, employee_count,
                            estimated_revenue, city, state, website,
                            rb2b_last_seen_at, rb2b_first_seen_at,
                            most_recent_referrer, recent_page_count, recent_page_urls,
                            profile_type, source_file, imported_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        v.visitor_id, v.email, v.first_name, v.last_name,
                        v.linkedin_url, v.company_name, v.title, v.industry,
                        v.employee_count, v.estimated_revenue, v.city, v.state,
                        v.website, v.rb2b_last_seen_at, v.rb2b_first_seen_at,
                        v.most_recent_referrer, v.recent_page_count, v.recent_page_urls,
                        v.profile_type, v.source_file, now,
                    ),
                )
            else:
                self._conn.execute(
                    """UPDATE rb2b_visitors
                       SET email=?, first_name=?, last_name=?, linkedin_url=?,
                           company_name=?, title=?, industry=?, employee_count=?,
                           estimated_revenue=?, city=?, state=?, website=?,
                           rb2b_last_seen_at=?, rb2b_first_seen_at=?,
                           most_recent_referrer=?, recent_page_count=?,
                           recent_page_urls=?, profile_type=?, source_file=?
                       WHERE visitor_id=?
                    """,
                    (
                        v.email, v.first_name, v.last_name, v.linkedin_url,
                        v.company_name, v.title, v.industry, v.employee_count,
                        v.estimated_revenue, v.city, v.state, v.website,
                        v.rb2b_last_seen_at, v.rb2b_first_seen_at,
                        v.most_recent_referrer, v.recent_page_count,
                        v.recent_page_urls, v.profile_type, v.source_file,
                        v.visitor_id,
                    ),
                )
        self._conn.commit()
        return new

    def get_unemitted_rb2b_visitors(self) -> list[Rb2bVisitor]:
        """Return identified visitors (have email) that haven't had events emitted."""
        rows = self._conn.execute(
            """SELECT * FROM rb2b_visitors
               WHERE email != '' AND email IS NOT NULL AND event_emitted_at IS NULL"""
        ).fetchall()
        return [_row_to_rb2b(r) for r in rows]

    def mark_rb2b_event_emitted(self, visitor_id: str) -> None:
        self._conn.execute(
            "UPDATE rb2b_visitors SET event_emitted_at=? WHERE visitor_id=?",
            (_now_iso(), visitor_id),
        )
        self._conn.commit()

    def rb2b_stats(self) -> dict[str, int]:
        total = self._conn.execute("SELECT COUNT(*) FROM rb2b_visitors").fetchone()[0]
        identified = self._conn.execute(
            "SELECT COUNT(*) FROM rb2b_visitors WHERE email != '' AND email IS NOT NULL"
        ).fetchone()[0]
        emitted = self._conn.execute(
            "SELECT COUNT(*) FROM rb2b_visitors WHERE event_emitted_at IS NOT NULL"
        ).fetchone()[0]
        return {"total": total, "identified": identified, "events_emitted": emitted}

    # ------------------------------------------------------------------
    # Runs — job tracking
    # ------------------------------------------------------------------

    def create_run(self, command: str, args: list[str]) -> int:
        """Insert a new run record (status='running') and return its id."""
        cur = self._conn.execute(
            "INSERT INTO runs (command, args, started_at, status) VALUES (?, ?, ?, 'running')",
            (command, json.dumps(args), _now_iso()),
        )
        self._conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def update_run(
        self,
        run_id: int,
        *,
        pid: int | None = None,
        status: str,
        exit_code: int | None = None,
        finished_at: str | None = None,
    ) -> None:
        self._conn.execute(
            """UPDATE runs
               SET pid=?, status=?, exit_code=?, finished_at=?
               WHERE id=?""",
            (pid, status, exit_code, finished_at, run_id),
        )
        self._conn.commit()

    def recent_runs(self, limit: int = 10) -> list["RunRecord"]:
        rows = self._conn.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            RunRecord(
                id=r["id"],
                command=r["command"],
                args=json.loads(r["args"] or "[]"),
                pid=r["pid"],
                started_at=r["started_at"] or "",
                finished_at=r["finished_at"] or "",
                exit_code=r["exit_code"],
                status=r["status"] or "running",
            )
            for r in rows
        ]

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


@dataclass
class RunRecord:
    id: int
    command: str
    args: list[str]
    pid: int | None
    started_at: str
    finished_at: str
    exit_code: int | None
    status: str  # running | done | failed | interrupted


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_rb2b(r: sqlite3.Row) -> Rb2bVisitor:
    return Rb2bVisitor(
        visitor_id=r["visitor_id"],
        email=r["email"] or "",
        first_name=r["first_name"] or "",
        last_name=r["last_name"] or "",
        linkedin_url=r["linkedin_url"] or "",
        company_name=r["company_name"] or "",
        title=r["title"] or "",
        industry=r["industry"] or "",
        employee_count=r["employee_count"] or "",
        estimated_revenue=r["estimated_revenue"] or "",
        city=r["city"] or "",
        state=r["state"] or "",
        website=r["website"] or "",
        rb2b_last_seen_at=r["rb2b_last_seen_at"] or "",
        rb2b_first_seen_at=r["rb2b_first_seen_at"] or "",
        most_recent_referrer=r["most_recent_referrer"] or "",
        recent_page_count=r["recent_page_count"] or "",
        recent_page_urls=r["recent_page_urls"] or "",
        profile_type=r["profile_type"] or "",
        source_file=r["source_file"] or "",
    )
