```
   _____ _                   ________                    
  / ___/(_)___ _____  ____ _/ / ____/___  _________ ____ 
  \__ \/ / __ `/ __ \/ __ `/ / /_  / __ \/ ___/ __ `/ _ \
 ___/ / / /_/ / / / / /_/ / / __/ / /_/ / /  / /_/ /  __/
/____/_/\__, /_/ /_/\__,_/_/_/    \____/_/   \__, /\___/ 
       /____/                               /____/       
```

> **Mine developer signals. Enrich with emails. Fire into your CRM.**

🟣 [PyPI v0.7.1](https://pypi.org/project/signalforge-cli/0.7.1/) &nbsp;|&nbsp; 🐍 Python 3.11+ &nbsp;|&nbsp; 📄 MIT &nbsp;|&nbsp; ⚡ [Built on Backboard.io](https://backboard.io) &nbsp;|&nbsp; 📬 [Customer.io](https://customer.io) &nbsp;|&nbsp; 🏆 [Devpost](https://devpost.com)

SignalForge scrapes Devpost hackathons, GitHub forks, and RB2B visitor exports — enriches every lead with real emails — then fires them straight into Customer.io. One command. Hundreds of warm leads.

---

## What's inside

| Command | What it does |
|---|---|
| `signalforge-devpost-search` | Search Devpost by keyword → enrich with emails → export CSV |
| `signalforge-participants` | Scrape one hackathon's participants → CSV |
| `signalforge-harvest` | Walk the full hackathon listing → SQLite → delta Customer.io events |
| `signalforge-github-forks` | Mine fork owners from any GitHub repo → emails → SQLite |
| `signalforge-gh-search` | Search GitHub repos by keyword → collect owner emails → SQLite |
| `signalforge-rb2b` | Import RB2B visitor CSVs → SQLite → `visited_site` events |
| `signalforge-auto` | **Full daily scrape**: RB2B today + open hackathons + all tracked GitHub repos (no emit) |
| `signalforge-emit-all` | Flush **every** unsent event across all sources in one shot |
| `signalforge-emit-batch` | Emit up to `--batch-size` events per source bucket — cron-friendly |
| `signalforge-auto-batch` | **One cron command**: daily scrape + emit batch in a single run |
| `signalforge-lookup` | Search the DB by email, name, or username — show full lead context |
| `signalforge-assistant` | Interactive AI analyst REPL over your lead database |
| `signalforge-campaigns` | Sync email HTML files with Customer.io campaign actions via the App API |

---

## Install

```bash
pip install signalforge-cli
```

Or with `uv` (recommended for local dev):

```bash
uv sync
```

---

## 30-second quickstart

```bash
# 1. Copy env and fill in your keys
cp .env.example .env

# 2. Search Devpost and get a CSV of leads with emails
signalforge-devpost-search "ai agents" -o leads.csv

# 3. Scrape all open hackathons, enrich new participants, emit to Customer.io
signalforge-harvest --emit-events
```

---

## How it works

```
  Devpost / GitHub / RB2B
         │
         ▼
  ┌─────────────────────┐
  │   fast scan / search │  (no enrichment yet — just IDs)
  └──────────┬──────────┘
             │
             ▼
  ┌─────────────────────┐
  │   SQLite upsert      │  detect NEW rows only (delta)
  └──────────┬──────────┘
             │
             ▼
  ┌─────────────────────┐
  │   email enrichment   │  GitHub API → profile walking → regex
  └──────────┬──────────┘
             │
             ▼
  ┌─────────────────────┐
  │   Customer.io emit   │  identify + track  (once per lead, ever)
  └─────────────────────┘
```

Delta logic: on re-runs, only *new* participants get the expensive enrichment. Already-emitted leads are never re-fired. Safe to run on a cron.

---

## Environment

Copy `.env.example` → `.env`:

| Variable | Required for | Notes |
|---|---|---|
| `BACKBOARD_API_KEY` | `signalforge` | Backboard account key |
| `DEVPOST_ASSISTANT_ID` | auto | Saved on first run, reused after |
| `DEVPOST_SESSION` | `signalforge-participants`, `signalforge-harvest` | `_devpost` cookie from browser DevTools |
| `GITHUB_TOKEN` | optional | PAT for 5 000 req/hr vs 60. Zero scopes needed |
| `CUSTOMERIO_SITE_ID` | `--emit-events` | Customer.io Track API (event emission) |
| `CUSTOMERIO_API_KEY` | `--emit-events` | Customer.io Track API (event emission) |
| `CUSTOMERIO_APP_API_KEY` | `signalforge-campaigns` | Customer.io App API (campaign management) |

---

## Commands

### `signalforge-devpost-search` — Devpost project search

Search Devpost by keyword, enrich each hit with the detail page + author email, export CSV.

```bash
signalforge-devpost-search "ai agents" --output results.csv
signalforge-devpost-search "climate tech" "developer tools" -o results.csv
```

---

### `signalforge-participants` — single hackathon

Scrape one hackathon's full participant list.

```bash
# First time — hand over your session cookie
signalforge-participants "https://authorizedtoact.devpost.com/participants" \
  --jwt "<_devpost cookie value>" -o participants.csv

# Subsequent runs — reuses saved session
signalforge-participants "https://authorizedtoact.devpost.com/participants" -o out.csv

# Fast mode (skip email enrichment)
signalforge-participants "https://..." --no-email -o out.csv

# Enrich + emit to Customer.io in one shot
signalforge-participants "https://..." --emit-events -o out.csv
```

---

### `signalforge-harvest` — full automated pipeline

Walks the Devpost hackathon listing, scrapes every participant, stores in SQLite, and emits Customer.io events for net-new leads.

```bash
# Standard run — open hackathons, 3 pages, enrich + emit
signalforge-harvest --emit-events

# Bulk first scrape without enrichment (fast)
signalforge-harvest --pages 5 --no-email

# Catch up: emit all unsent leads already in the DB (no scraping needed)
signalforge-harvest --emit-unsent

# Re-scan for new joiners, enrich + emit delta
signalforge-harvest --rescrape --emit-events

# Include ended hackathons too
signalforge-harvest --status open --status ended --pages 5

# Export everyone who has a LinkedIn URL but no email yet (CSV for manual outreach)
signalforge-harvest --export-linkedin -o linkedin_leads.csv
```

**Flags**

| Flag | Default | Description |
|---|---|---|
| `--pages N` | `3` | Hackathon listing pages (9 hackathons each) |
| `--hackathons N` | `0` (all) | Stop after the first N hackathons |
| `--max-participants N` | `0` (unlimited) | Cap per hackathon |
| `--jwt TOKEN` | `.env` | Devpost `_devpost` session cookie |
| `--db PATH` | `devpost_harvest.db` | SQLite path |
| `--status` | `open` | `open` / `ended` / `upcoming` (repeatable) |
| `--no-email` | off | Skip enrichment entirely |
| `--emit-events` | off | Emit Customer.io events for delta participants |
| `--emit-unsent` | off | Just emit — no scraping |
| `--rescrape` | off | Re-scrape already-seen hackathons |
| `--export-linkedin` | off | Export CSV of all leads with LinkedIn but no email |
| `--output / -o PATH` | stdout | Output path for `--export-linkedin` |

**SQLite schema**

- **`hackathons`** — url, title, org, state, dates, registrations, prize, themes, `last_scraped_at`
- **`participants`** — `(hackathon_url, username)` PK + enrichment fields + `first_seen_at`, `last_seen_at`, `event_emitted_at`

**Customer.io events**

Event name depends on how old the hackathon is:

| Condition | Event name |
|---|---|
| Hackathon is open, or closed within the last 30 days | `devpost_hackathon` |
| Hackathon closed more than 30 days ago | `closed_hackathon` |

Email = Customer.io user ID. Payload: `hackathon_url`, `hackathon_title`, `username`, `name`, `specialty`, `profile_url`, `github_url`, `linkedin_url`.

Email templates: `emails/devpost-hackathon/` (variants a–l) for `devpost_hackathon`, `emails/closed-hackathon/` (variants a–f, MLH free-tier campaign) for `closed_hackathon`. All use `{{customer.first_name}}` and `{{event.*}}` Liquid variables. Push changes to cx.io with `signalforge-campaigns update-all`.

---

### `signalforge-github-forks` — GitHub fork mining

Pull every fork owner from a repo, enrich with public emails, store in the same SQLite DB.

```bash
# Built-in presets
signalforge-github-forks --preset mem0 --emit-events
signalforge-github-forks --preset supermemory --no-email

# Any repo
signalforge-github-forks --repo owner/repo --limit 1000 --mode first_n
```

| Flag | Default | Description |
|---|---|---|
| `--preset` | — | `mem0` or `supermemory` shorthand |
| `--repo OWNER/REPO` | — | Any public GitHub repo |
| `--limit N` | `2000` | Max forks to process |
| `--mode` | preset-dependent | `top_by_pushed` or `first_n` |
| `--no-email` | off | Skip email lookup |
| `--emit-events` | off | Emit Customer.io events |
| `--force-email` | off | Re-enrich all forks, not just new ones |

---

### `signalforge-auto` — full daily scrape

Runs all three scrapers in sequence, then exits without emitting events. Use this as your daily cron job; fire `--emit-unsent` on each source afterwards.

**What it runs:**
1. `signalforge-rb2b --fetch-date TODAY` — pulls today's RB2B visitor export
2. `signalforge-harvest --status open --pages 100` — walks all open Devpost hackathons
3. `signalforge-github-forks --repo OWNER/REPO --limit 5000` — for every repo already tracked in the DB

```bash
# Standard daily run
signalforge-auto

# Custom date / page depth
signalforge-auto --fetch-date 2026-03-31 --pages 50 --fork-limit 2000

# Skip email enrichment (much faster, enrich later with --force-email)
signalforge-auto --no-email

# Then flush the queue when ready
signalforge-harvest --emit-unsent
signalforge-github-forks --emit-unsent
signalforge-rb2b --emit-unsent
```

| Flag | Default | Description |
|---|---|---|
| `--db PATH` | `devpost_harvest.db` | SQLite path |
| `--pages N` | `100` | Devpost listing pages |
| `--fork-limit N` | `5000` | Max forks per GitHub repo |
| `--fetch-date YYYY-MM-DD` | today | RB2B export date |
| `--no-email` | off | Skip email enrichment |
| `--jwt TOKEN` | `.env` | Devpost session cookie |

---

### `signalforge-rb2b` — RB2B visitor import

Load RB2B daily export CSVs and fire `visited_site` events for identified visitors.

```bash
# Import and emit new identified visitors
signalforge-rb2b daily_2026-03-*.csv --emit-events

# Just drain the unsent queue
signalforge-rb2b --emit-unsent
```

---

### `signalforge-gh-search` — GitHub repo search

Search GitHub repos by keyword, collect owner emails via the GitHub API, and store results in the harvest DB (`hackathon_url = github:search:<query-slug>`).

```bash
# Search and enrich owners
signalforge-gh-search "ai memory" --max 200 --emit-events

# Top results by stars (default), forks, or recency
signalforge-gh-search "langchain rag" --sort forks --max 500

# Skip enrichment now, drain later
signalforge-gh-search "vector database" --no-email
signalforge-gh-search --emit-unsent
```

| Flag | Default | Description |
|---|---|---|
| `query` | — | GitHub search query (e.g. `"AI memory"`) |
| `--max N` | `100` | Max repos to retrieve (GitHub caps at 1000) |
| `--sort` | `stars` | `stars` / `forks` / `updated` |
| `--db PATH` | `devpost_harvest.db` | SQLite path |
| `--no-email` | off | Skip email enrichment |
| `--force-email` | off | Re-enrich owners already in the DB |
| `--emit-events` | off | Emit `github_search` events to Customer.io |
| `--emit-limit N` | `0` (all) | Cap `--emit-events` to N owners |
| `--emit-unsent` | off | Skip search — flush unsent queue only |

---

### `signalforge-emit-all` — flush all unsent events

Drain the unsent queue for every source in one shot: Devpost hackathons, GitHub fork owners, GitHub search owners, and RB2B visitors.

```bash
signalforge-emit-all
signalforge-emit-all --db my_harvest.db
```

| Flag | Default | Description |
|---|---|---|
| `--db PATH` | `devpost_harvest.db` | SQLite path |

---

### `signalforge-auto-batch` — daily scrape + emit in one command

The single cron entry you actually need. Runs the full daily scrape (`signalforge-auto`) and then immediately emits one batch from every source bucket (`signalforge-emit-batch`).

```bash
# Add to crontab — runs at 6 AM daily
0 6 * * * /path/to/venv/bin/signalforge-auto-batch >> /var/log/signalforge.log 2>&1

# Manual run with defaults
signalforge-auto-batch

# Smaller emit batch (useful during warm-up while the queue is large)
signalforge-auto-batch --batch-size 500

# Skip email enrichment for a faster run
signalforge-auto-batch --no-email --batch-size 1000
```

| Flag | Default | Description |
|---|---|---|
| `--batch-size N` | `2000` | Max events to emit per source bucket after scrape |
| `--pages N` | `100` | Devpost listing pages to fetch |
| `--fork-limit N` | `5000` | Max forks per GitHub repo |
| `--fetch-date YYYY-MM-DD` | today | RB2B export date |
| `--no-email` | off | Skip email enrichment |
| `--jwt TOKEN` | `.env` | Devpost session cookie |
| `--db PATH` | `devpost_harvest.db` | SQLite path |

---

### `signalforge-emit-batch` — batched emit (cron-friendly)

Emit up to `--batch-size` events from each of four source buckets in a single run. Unlike `signalforge-emit-all` (which drains the entire queue), this lets you pace delivery — run it on a cron until the queue is empty.

| Bucket | Event name | Source |
|---|---|---|
| Devpost open / recently-closed (≤30 days) | `devpost_hackathon` | harvest DB |
| Devpost old-closed (>30 days) | `closed_hackathon` | harvest DB |
| GitHub fork owners | `github_fork` | harvest DB |
| RB2B identified visitors | `visited_site` | harvest DB |

```bash
# Emit up to 2000 per bucket (default)
signalforge-emit-batch

# Smaller batches — good for warming up or rate-limiting
signalforge-emit-batch --batch-size 500

# Custom DB
signalforge-emit-batch --batch-size 1000 --db my_harvest.db
```

| Flag | Default | Description |
|---|---|---|
| `--batch-size N` | `2000` | Max events to emit per source bucket |
| `--db PATH` | `devpost_harvest.db` | SQLite path |

---

### `signalforge-lookup` — contact lookup

Search the SQLite DB by email address, name, or username and print the full lead record with hackathon context.

```bash
signalforge-lookup alice@example.com
signalforge-lookup "Alice Smith"
signalforge-lookup alicedev
signalforge-lookup alice --db my_harvest.db
```

| Flag | Default | Description |
|---|---|---|
| `query` | — | Email, name, or username to search |
| `--db PATH` | `devpost_harvest.db` | SQLite path |

---

### `signalforge-assistant` — AI analyst REPL

Interactive natural-language interface to your lead database powered by a Backboard AI assistant. Ask questions, query leads, and get summaries without writing SQL.

```bash
signalforge-assistant
signalforge-assistant --db my_harvest.db
```

```
> How many participants have emails from the last 30 days?
> Show me leads from AI-themed hackathons with a GitHub URL
> Which hackathons have the most unemitted participants?
```

| Flag | Default | Description |
|---|---|---|
| `--db PATH` | `devpost_harvest.db` | SQLite path |

Requires `BACKBOARD_API_KEY`. Model defaults to `gpt-4o-mini`; override with `BACKBOARD_MODEL` and `BACKBOARD_LLM_PROVIDER`.

---

### `signalforge-campaigns` — Customer.io campaign management

Sync HTML email files with Customer.io campaign actions via the App API. Eliminates copy-pasting: edit locally, push with one command.

**Email templates** live in `emails/` organised by campaign:

```
emails/
├── manifest.json                  ← global action ↔ file registry
├── campaigns/                     ← per-campaign fetched manifests
│   └── {campaign_id}.json
├── devpost-hackathon/             ← variants for the devpost_hackathon event
│   ├── variant-a.html … variant-l.html
├── closed-hackathon/              ← variants for the closed_hackathon event (MLH free tier)
│   ├── variant-a.html … variant-f.html
└── github-forkers/                ← variants for the github_fork event
    └── variant-a.html … variant-h.html
```

Every HTML file has a `<!-- Subject: ... -->` comment at line 1 — that becomes the email subject on push. Liquid variables `{{customer.first_name}}` and `{{event.*}}` are used throughout.

**Typical workflow:**

```bash
# 1. See all campaigns
signalforge-campaigns list-campaigns

# 2. Pull a campaign + all its actions into emails/campaigns/{id}.json
signalforge-campaigns get-campaign --campaign-id 9

# 3. Visualise the action graph as a Mermaid flowchart
signalforge-campaigns show-campaign --campaign-id 9

# 4. Auto-pair all email actions to a folder of HTML files (checks counts match)
signalforge-campaigns get-actions --campaign-id 9 --folder emails/closed-hackathon

# 5. Push all files for a campaign at once
signalforge-campaigns update-all --campaign-id 9

# One-off: register or push a single action
signalforge-campaigns get    --campaign-id 9 --action-id 353 --file emails/closed-hackathon/variant-c.html
signalforge-campaigns update --file emails/closed-hackathon/variant-c.html
```

**Subcommands**

| Subcommand | Description |
|---|---|
| `list-campaigns` | List all campaigns — id, state, name |
| `get-campaign` | Fetch a campaign + all its actions → `emails/campaigns/{id}.json` |
| `show-campaign` | Print a Mermaid flowchart of the campaign's action graph |
| `get-actions` | Fetch all email actions, pair with a local folder, upsert `manifest.json` |
| `update-all` | Push every manifest-linked HTML file for a campaign to cx.io |
| `get` | Fetch one action and upsert it into `manifest.json` |
| `update` | Push one local HTML file's subject + body to cx.io |

**`get-actions` count validation**

Before writing anything, `get-actions` compares the number of email-type actions in the campaign (A/B test container nodes are automatically excluded) against the number of HTML files in the folder. If they don't match, it prints both lists and exits — nothing is written.

```bash
# Skip the confirmation prompt for scripting / CI
signalforge-campaigns get-actions --campaign-id 9 --folder emails/closed-hackathon --yes
```

**`manifest.json` schema**

```json
[
  {
    "file":            "emails/closed-hackathon/variant-c.html",
    "campaign_id":     "9",
    "action_id":       "353",
    "name":            "Email 1",
    "subject":         "Late to your inbox, but this is worth it",
    "last_fetched_at": "2026-04-02T18:51:27+00:00",
    "last_pushed_at":  "2026-04-02T18:51:51+00:00"
  }
]
```

Commit `manifest.json` alongside the HTML files so the team can always see which version is live in cx.io and when it was last pushed.

---

## Requirements

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) (for local dev)
- [Backboard](https://app.backboard.io) API key (for `signalforge-devpost-search` keyword search only)

---

## Development

```bash
uv run python -m devpost_scraper.cli "ai agents" --output out.csv
```
