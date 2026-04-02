"""Customer.io App API client for managing campaign action email content.

Subcommands
───────────
  list-campaigns  — list all campaigns (id + name)
  get-campaign    — fetch one campaign + all its actions → emails/campaigns/{id}.json
  show-campaign   — print a Mermaid flowchart of a campaign's action graph
  get             — fetch one action and upsert it into emails/manifest.json
  update          — push a local HTML file's subject + body to cx.io

Per-campaign manifest: emails/campaigns/{campaign_id}.json
Global action manifest: emails/manifest.json (one entry per action ↔ local file mapping)
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

_APP_API_BASE = "https://api.customer.io/v1"
_MANIFEST_PATH = Path("emails/manifest.json")
_CAMPAIGNS_DIR = Path("emails/campaigns")


# ── App API client ────────────────────────────────────────────────────────────

class CampaignClient:
    def __init__(self, app_api_key: str) -> None:
        self._headers = {
            "Authorization": f"Bearer {app_api_key}",
            "Content-Type": "application/json",
        }

    def _get(self, path: str) -> dict[str, Any]:
        resp = httpx.get(f"{_APP_API_BASE}{path}", headers=self._headers, timeout=15.0)
        if resp.status_code != 200:
            raise RuntimeError(f"[cx.io] GET {path} → {resp.status_code} {resp.text}")
        return resp.json()

    def list_campaigns(self) -> list[dict[str, Any]]:
        """GET /campaigns — returns all campaigns."""
        data = self._get("/campaigns")
        return data.get("campaigns", [])

    def get_campaign(self, campaign_id: str) -> dict[str, Any]:
        """GET /campaigns/{id} — returns full campaign object."""
        data = self._get(f"/campaigns/{campaign_id}")
        return data.get("campaign", data)

    def list_actions(self, campaign_id: str) -> list[dict[str, Any]]:
        """GET /campaigns/{id}/actions — returns all actions for a campaign."""
        data = self._get(f"/campaigns/{campaign_id}/actions")
        return data.get("actions", [])

    def get_action(self, campaign_id: str, action_id: str) -> dict[str, Any]:
        data = self._get(f"/campaigns/{campaign_id}/actions/{action_id}")
        return data.get("action", data)

    def update_action(
        self,
        campaign_id: str,
        action_id: str,
        subject: str,
        body: str,
    ) -> dict[str, Any]:
        url = f"{_APP_API_BASE}/campaigns/{campaign_id}/actions/{action_id}"
        payload = {"subject": subject, "body": body}
        resp = httpx.put(url, headers=self._headers, json=payload, timeout=15.0)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"[cx.io] PUT action {campaign_id}/{action_id} → "
                f"{resp.status_code} {resp.text}"
            )
        return resp.json().get("action", resp.json())


def _build_client() -> CampaignClient:
    key = os.getenv("CUSTOMERIO_APP_API_KEY", "").strip()
    if not key:
        raise SystemExit("[error] CUSTOMERIO_APP_API_KEY must be set in .env")
    return CampaignClient(key)


# ── HTML subject parser ───────────────────────────────────────────────────────

def parse_subject(html: str) -> str:
    """Extract subject from the leading HTML comment: <!-- Subject: ... -->"""
    m = re.search(r"<!--\s*Subject:\s*(.+?)\s*-->", html, re.IGNORECASE)
    if not m:
        raise ValueError(
            "No subject comment found. Add <!-- Subject: ... --> at the top of the file."
        )
    return m.group(1).strip()


# ── Manifest helpers ──────────────────────────────────────────────────────────

def load_manifest(path: Path = _MANIFEST_PATH) -> list[dict]:
    if path.exists():
        return json.loads(path.read_text())
    return []


def save_manifest(entries: list[dict], path: Path = _MANIFEST_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, indent=2) + "\n")


def _find_entry(
    entries: list[dict], campaign_id: str, action_id: str
) -> dict | None:
    for e in entries:
        if str(e.get("campaign_id")) == str(campaign_id) and str(e.get("action_id")) == str(action_id):
            return e
    return None


def _find_entry_by_file(entries: list[dict], file: str) -> dict | None:
    for e in entries:
        if e.get("file") == file:
            return e
    return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Formatting helpers ────────────────────────────────────────────────────────

_ACTION_ICONS: dict[str, str] = {
    "email_action": "📧",
    "email": "📧",
    "sms_action": "💬",
    "sms": "💬",
    "webhook_action": "🔗",
    "webhook": "🔗",
    "push_action": "🔔",
    "push": "🔔",
    "in_app_action": "📱",
    "slack_action": "🔷",
}


def _action_icon(action_type: str) -> str:
    return _ACTION_ICONS.get(action_type.lower(), "⚡")


def _fmt_delay(seconds: int | None) -> str:
    if not seconds:
        return "immediately"
    if seconds < 3600:
        m = seconds // 60
        return f"+{m} min" if m else f"+{seconds}s"
    if seconds < 86400:
        return f"+{seconds // 3600}h"
    return f"+{seconds // 86400}d"


def _truncate(s: str, n: int = 48) -> str:
    s = s.strip()
    return s[:n] + "…" if len(s) > n else s


def _mermaid_id(action_id: Any) -> str:
    return f"A{action_id}".replace("-", "_")


# ── Per-campaign manifest ─────────────────────────────────────────────────────

def _campaign_manifest_path(campaign_id: str) -> Path:
    return _CAMPAIGNS_DIR / f"{campaign_id}.json"


def _load_global_manifest() -> list[dict]:
    return load_manifest()


def _file_for_action(global_entries: list[dict], campaign_id: str, action_id: str) -> str:
    """Look up the linked local file from the global manifest, if any."""
    for e in global_entries:
        if str(e.get("campaign_id")) == str(campaign_id) and str(e.get("action_id")) == str(action_id):
            return e.get("file", "")
    return ""


# ── Public commands ───────────────────────────────────────────────────────────

def cmd_list_campaigns() -> None:
    """Print a table of all campaigns: id, state, name."""
    from rich.console import Console
    from rich.table import Table

    client = _build_client()
    print("[campaigns] Fetching campaign list …", file=sys.stderr)
    campaigns = client.list_campaigns()

    console = Console()
    table = Table(show_header=True, header_style="bold cyan", box=None, pad_edge=False)
    table.add_column("ID", style="dim", width=12)
    table.add_column("State", width=10)
    table.add_column("Name")

    state_colours = {"draft": "yellow", "sent": "green", "stopped": "red", "paused": "magenta"}
    for c in sorted(campaigns, key=lambda x: x.get("id", 0)):
        state = c.get("state", "")
        colour = state_colours.get(state, "white")
        table.add_row(
            str(c.get("id", "")),
            f"[{colour}]{state}[/{colour}]",
            c.get("name", "(unnamed)"),
        )

    console.print(table)
    print(f"\n{len(campaigns)} campaigns", file=sys.stderr)


def cmd_get_campaign(campaign_id: str) -> None:
    """Fetch a campaign + its actions and write emails/campaigns/{id}.json."""
    client = _build_client()
    global_entries = _load_global_manifest()

    print(f"[campaigns] Fetching campaign {campaign_id} …", file=sys.stderr)
    campaign = client.get_campaign(campaign_id)
    actions = client.list_actions(campaign_id)

    enriched_actions = []
    for a in actions:
        aid = str(a.get("id", ""))
        enriched_actions.append({
            "action_id": aid,
            "name": a.get("name", ""),
            "type": a.get("type", ""),
            "subject": a.get("subject", ""),
            "delay": a.get("delay_seconds", a.get("delay", 0)),
            "parent_action_id": str(a.get("parent_action_id", "")) if a.get("parent_action_id") else None,
            "file": _file_for_action(global_entries, campaign_id, aid),
        })

    manifest = {
        "campaign_id": str(campaign_id),
        "name": campaign.get("name", ""),
        "state": campaign.get("state", ""),
        "type": campaign.get("type", ""),
        "fetched_at": _now(),
        "actions": enriched_actions,
    }

    out_path = _campaign_manifest_path(str(campaign_id))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"  name:    {manifest['name']}", file=sys.stderr)
    print(f"  state:   {manifest['state']}", file=sys.stderr)
    print(f"  actions: {len(enriched_actions)}", file=sys.stderr)
    print(f"[campaigns] Written → {out_path}", file=sys.stderr)


def cmd_show_campaign(campaign_id: str) -> None:
    """Print a Mermaid flowchart of the campaign's action graph to stdout."""
    manifest_path = _campaign_manifest_path(str(campaign_id))

    if not manifest_path.exists():
        raise SystemExit(
            f"[error] No manifest found for campaign {campaign_id}.\n"
            f"Run first: signalforge-campaigns get-campaign --campaign-id {campaign_id}"
        )

    manifest = json.loads(manifest_path.read_text())
    actions = manifest.get("actions", [])
    campaign_name = manifest.get("name", campaign_id)
    trigger_type = manifest.get("type", "trigger")

    lines: list[str] = ["```mermaid", "flowchart TD"]

    # Trigger node
    lines.append(f'    TRIGGER(["⚡ Trigger: {trigger_type}\\n{campaign_name}"])')

    # Action nodes
    for a in actions:
        mid = _mermaid_id(a["action_id"])
        icon = _action_icon(a.get("type", ""))
        name = _truncate(a.get("name") or a["action_id"], 30)
        subject = _truncate(a.get("subject", ""), 44)
        delay = _fmt_delay(a.get("delay") or 0)
        label = f'{icon} {name}\\n{subject}\\n⏱ {delay}'
        lines.append(f'    {mid}["{label}"]')

    lines.append("")

    # Edges — use parent_action_id when present, else chain from trigger
    has_parent = {a["action_id"] for a in actions if a.get("parent_action_id")}
    roots = [a for a in actions if not a.get("parent_action_id")]

    for a in roots:
        lines.append(f'    TRIGGER --> {_mermaid_id(a["action_id"])}')

    for a in actions:
        if a.get("parent_action_id"):
            src = _mermaid_id(a["parent_action_id"])
            dst = _mermaid_id(a["action_id"])
            delay = _fmt_delay(a.get("delay") or 0)
            lines.append(f'    {src} -->|"{delay}"| {dst}')

    # Style roots differently
    for a in roots:
        lines.append(f'    style {_mermaid_id(a["action_id"])} fill:#dbeafe,stroke:#3b82f6')

    lines.append("```")
    print("\n".join(lines))


def _is_email_action(action: dict) -> bool:
    if "email" not in action.get("type", "").lower():
        return False
    # A/B test containers are split envelopes, not actual emails.
    # They have recipient="{{customer.email}}", empty layout, and no from_id.
    if action.get("recipient") == "{{customer.email}}":
        return False
    return True


def _natural_sort_key(s: str) -> list:
    """Sort strings with embedded numbers naturally: 'Email 2' < 'Email 10'."""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]


def cmd_get_actions(campaign_id: str, folder: str, yes: bool = False) -> None:
    """Fetch all email actions for a campaign, pair with folder HTML files, upsert manifest."""
    from rich.console import Console
    from rich.table import Table

    folder_path = Path(folder)
    if not folder_path.is_dir():
        raise SystemExit(f"[error] Folder not found: {folder}")

    html_files = sorted(folder_path.glob("*.html"), key=lambda p: _natural_sort_key(p.name))
    if not html_files:
        raise SystemExit(f"[error] No HTML files found in {folder}")

    client = _build_client()
    print(f"[campaigns] Fetching actions for campaign {campaign_id} …", file=sys.stderr)
    all_actions = client.list_actions(campaign_id)
    email_actions = [a for a in all_actions if _is_email_action(a)]
    email_actions.sort(key=lambda a: _natural_sort_key(a.get("name", str(a.get("id", "")))))

    # Count check
    n_actions, n_files = len(email_actions), len(html_files)
    if n_actions != n_files:
        print(
            f"[error] Mismatch: campaign has {n_actions} email action(s) "
            f"but {folder} has {n_files} HTML file(s).",
            file=sys.stderr,
        )
        print("\nEmail actions:", file=sys.stderr)
        for a in email_actions:
            print(f"  [{a.get('id')}] {a.get('name', '(unnamed)')}", file=sys.stderr)
        print("\nHTML files:", file=sys.stderr)
        for f in html_files:
            print(f"  {f.name}", file=sys.stderr)
        raise SystemExit(1)

    # Show proposed pairing
    console = Console(stderr=True)
    table = Table(
        title=f"Proposed pairing — campaign {campaign_id}",
        show_header=True, header_style="bold cyan", box=None, pad_edge=False,
    )
    table.add_column("Action ID", style="dim", width=12)
    table.add_column("Action name", width=24)
    table.add_column("←→")
    table.add_column("File")

    pairs = list(zip(email_actions, html_files))
    for action, html_path in pairs:
        table.add_row(
            str(action.get("id", "")),
            action.get("name", "(unnamed)"),
            "↔",
            str(html_path),
        )

    console.print(table)

    if not yes:
        try:
            answer = input("\nLook good? Upsert all into manifest.json? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            raise SystemExit("\nAborted.")
        if answer != "y":
            raise SystemExit("Aborted.")

    # Upsert each pair into the global manifest
    entries = load_manifest()
    now = _now()
    for action, html_path in pairs:
        aid = str(action.get("id", ""))
        file_str = str(html_path)
        existing = _find_entry(entries, campaign_id, aid)
        if existing:
            existing["name"] = action.get("name", "")
            existing["subject"] = action.get("subject", "")
            existing["file"] = file_str
            existing["last_fetched_at"] = now
        else:
            entries.append({
                "file": file_str,
                "campaign_id": str(campaign_id),
                "action_id": aid,
                "name": action.get("name", ""),
                "subject": action.get("subject", ""),
                "last_fetched_at": now,
                "last_pushed_at": None,
            })

    save_manifest(entries)
    print(
        f"[campaigns] manifest.json updated — {n_actions} action(s) linked "
        f"({len(entries)} total entries).",
        file=sys.stderr,
    )


def cmd_update_all(campaign_id: str) -> None:
    """Push all manifest entries for a campaign to cx.io."""
    from rich.console import Console
    from rich.table import Table

    entries = load_manifest()
    campaign_entries = [
        e for e in entries
        if str(e.get("campaign_id")) == str(campaign_id) and e.get("file")
    ]

    if not campaign_entries:
        raise SystemExit(
            f"[error] No manifest entries with files found for campaign {campaign_id}.\n"
            f"Run first: signalforge-campaigns get-actions --campaign-id {campaign_id} --folder <path>"
        )

    # Pre-flight: check all files exist
    missing = [e["file"] for e in campaign_entries if not Path(e["file"]).exists()]
    if missing:
        raise SystemExit(
            "[error] Missing files:\n" + "\n".join(f"  {f}" for f in missing)
        )

    client = _build_client()
    console = Console(stderr=True)
    now = _now()
    ok_count = 0

    for e in campaign_entries:
        html = Path(e["file"]).read_text(encoding="utf-8")
        try:
            subject = parse_subject(html)
        except ValueError as exc:
            print(f"  [skip] {e['file']}: {exc}", file=sys.stderr)
            continue

        print(
            f"  pushing [{e['action_id']}] {e['name'] or e['action_id']}  ←  {e['file']}",
            file=sys.stderr,
        )
        client.update_action(e["campaign_id"], e["action_id"], subject=subject, body=html)
        e["subject"] = subject
        e["last_pushed_at"] = now
        ok_count += 1

    save_manifest(entries)
    print(
        f"[campaigns] Done. {ok_count}/{len(campaign_entries)} action(s) pushed.",
        file=sys.stderr,
    )


def cmd_get(campaign_id: str, action_id: str, file: str | None) -> None:
    """Fetch one action from cx.io and upsert it into manifest.json."""
    client = _build_client()

    print(f"[campaigns] Fetching action {campaign_id}/{action_id} …", file=sys.stderr)
    action = client.get_action(campaign_id, action_id)

    subject = action.get("subject", "")
    name = action.get("name", "")
    remote_body = action.get("body", "")

    print(f"  name:    {name or '(unnamed)'}", file=sys.stderr)
    print(f"  subject: {subject or '(empty)'}", file=sys.stderr)
    print(f"  body:    {len(remote_body)} chars", file=sys.stderr)

    entries = load_manifest()
    existing = _find_entry(entries, campaign_id, action_id)

    if existing:
        existing["name"] = name
        existing["subject"] = subject
        existing["last_fetched_at"] = _now()
        if file:
            existing["file"] = file
    else:
        entries.append({
            "file": file or "",
            "campaign_id": str(campaign_id),
            "action_id": str(action_id),
            "name": name,
            "subject": subject,
            "last_fetched_at": _now(),
            "last_pushed_at": None,
        })

    save_manifest(entries)
    print(f"[campaigns] manifest.json updated ({len(entries)} entries total)", file=sys.stderr)

    if not file:
        print(
            "[campaigns] Tip: run again with --file emails/<folder>/variant-x.html "
            "to link this action to a local file.",
            file=sys.stderr,
        )


def cmd_update(file: str) -> None:
    """Read a local HTML file and push its subject + body to cx.io."""
    entries = load_manifest()
    entry = _find_entry_by_file(entries, file)
    if not entry:
        raise SystemExit(
            f"[error] '{file}' not found in manifest.json.\n"
            "Run: signalforge-campaigns get --campaign-id X --action-id Y --file <path>"
        )

    campaign_id = entry["campaign_id"]
    action_id = entry["action_id"]
    html_path = Path(file)

    if not html_path.exists():
        raise SystemExit(f"[error] File not found: {file}")

    html = html_path.read_text(encoding="utf-8")
    subject = parse_subject(html)

    print(f"[campaigns] Pushing '{file}' → action {campaign_id}/{action_id}", file=sys.stderr)
    print(f"  subject: {subject}", file=sys.stderr)
    print(f"  body:    {len(html)} chars", file=sys.stderr)

    client = _build_client()
    client.update_action(campaign_id, action_id, subject=subject, body=html)

    entry["subject"] = subject
    entry["last_pushed_at"] = _now()
    save_manifest(entries)

    print(f"[campaigns] Done. manifest.json updated with last_pushed_at.", file=sys.stderr)
