```
   _____ _                   ________                    
  / ___/(_)___ _____  ____ _/ / ____/___  _________ ____ 
  \__ \/ / __ `/ __ \/ __ `/ / /_  / __ \/ ___/ __ `/ _ \
 ___/ / / /_/ / / / / /_/ / / __/ / /_/ / /  / /_/ /  __/
/____/_/\__, /_/ /_/\__,_/_/_/    \____/_/   \__, /\___/ 
       /____/                               /____/       
```

> **Mine developer signals. Enrich with emails. Fire into your CRM.**

🟣 [PyPI v0.5.1](https://pypi.org/project/signalforge-cli/0.5.1/) &nbsp;|&nbsp; 🐍 Python 3.11+ &nbsp;|&nbsp; 📄 MIT &nbsp;|&nbsp; ⚡ [Built on Backboard.io](https://backboard.io) &nbsp;|&nbsp; 📬 [Customer.io](https://customer.io) &nbsp;|&nbsp; 🏆 [Devpost](https://devpost.com)

SignalForge scrapes Devpost hackathons, GitHub forks, and RB2B visitor exports — enriches every lead with real emails — then fires them straight into Customer.io. One command. Hundreds of warm leads.

---

## What's inside

| Command | What it does |
|---|---|
| `signalforge-devpost-search` | Search Devpost by keyword → enrich with emails → export CSV |
| `signalforge-participants` | Scrape one hackathon's participants → CSV |
| `signalforge-harvest` | Walk the full hackathon listing → SQLite → delta Customer.io events |
| `signalforge-github-forks` | Mine fork owners from any GitHub repo → emails → SQLite |
| `signalforge-rb2b` | Import RB2B visitor CSVs → SQLite → `visited_site` events |
| `signalforge-auto` | **Full daily scrape**: RB2B today + open hackathons + all tracked GitHub repos (no emit) |

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
| `CUSTOMERIO_SITE_ID` | `--emit-events` | Customer.io Track API |
| `CUSTOMERIO_API_KEY` | `--emit-events` | Customer.io Track API |

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

Event name: `devpost_hackathon`. Email = Customer.io user ID.
Payload: `hackathon_url`, `hackathon_title`, `username`, `name`, `specialty`, `profile_url`, `github_url`, `linkedin_url`.

Email templates in `emails/` use `{{customer.first_name}}` and `{{event.*}}` Liquid variables.

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

## Requirements

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) (for local dev)
- [Backboard](https://app.backboard.io) API key (for `signalforge-devpost-search` keyword search only)

---

## Development

```bash
uv run python -m devpost_scraper.cli "ai agents" --output out.csv
```
