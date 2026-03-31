# Devpost Scraper

CLI toolkit for extracting Devpost hackathon data, enriching participants with emails,
storing results in SQLite, and emitting Customer.io events.

Three commands:

| Command | Purpose |
|---|---|
| `devpost-scraper` | Search Devpost projects by keyword, enrich with emails, export CSV |
| `devpost-participants` | Scrape a single hackathon's participant list, export CSV |
| `devpost-harvest` | Walk the hackathon listing, scrape all participants, store in SQLite, emit delta events |

## Requirements

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/)
- A [Backboard](https://app.backboard.io) API key (for `devpost-scraper` only)

## Install

```bash
uv sync
```

## Environment

Copy `.env.example` → `.env` and fill in:

| Variable | Required for | Notes |
|---|---|---|
| `BACKBOARD_API_KEY` | `devpost-scraper` | Backboard account key |
| `DEVPOST_ASSISTANT_ID` | auto | Persisted on first run |
| `DEVPOST_SESSION` | `devpost-participants`, `devpost-harvest` | `_devpost` cookie from browser DevTools |
| `GITHUB_TOKEN` | optional | GitHub PAT for 5000 req/hr (vs 60). No scopes needed |
| `CUSTOMERIO_SITE_ID` | `--emit-events` | Customer.io Track API |
| `CUSTOMERIO_API_KEY` | `--emit-events` | Customer.io Track API |

---

## devpost-scraper

Search Devpost projects by keyword, enrich each with detail page + author email, export CSV.

```bash
uv run devpost-scraper "ai agents" --output results.csv
uv run devpost-scraper "climate tech" "developer tools" -o results.csv

# Or via start.sh
./start.sh "ai agents" --output results.csv
```

---

## devpost-participants

Scrape a single hackathon's participant list and export to CSV.

```bash
# First time — pass session cookie
uv run devpost-participants "https://authorizedtoact.devpost.com/participants" \
  --jwt "<_devpost cookie value>" -o participants.csv

# Reuse saved session from .env
uv run devpost-participants "https://authorizedtoact.devpost.com/participants" -o out.csv

# Skip email enrichment
uv run devpost-participants "https://..." --no-email -o out.csv

# Emit Customer.io events after scrape
uv run devpost-participants "https://..." --emit-events -o out.csv
```

---

## devpost-harvest

Automated pipeline: walk the hackathon listing → scrape participants → store in SQLite → emit Customer.io events for delta (new) participants.

### Basic usage

```bash
# Scrape 3 pages of open hackathons (27 hackathons), enrich new participants, emit events
uv run devpost-harvest --emit-events

# Fast first run — scrape without email enrichment
uv run devpost-harvest --no-email
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--pages N` | `3` | Number of hackathon listing pages to fetch (9 per page) |
| `--hackathons N` | `0` (all) | Only process the first N hackathons from the listing |
| `--jwt TOKEN` | `.env` | Devpost `_devpost` session cookie |
| `--db PATH` | `devpost_harvest.db` | SQLite database path |
| `--status {open,ended,upcoming}` | `open` | Hackathon status filter (repeatable) |
| `--max-participants N` | `0` (unlimited) | Cap participants scraped per hackathon |
| `--no-email` | off | Skip email enrichment entirely (even for new participants) |
| `--emit-events` | off | Emit Customer.io events for unemitted participants during scrape |
| `--emit-unsent` | off | Skip scraping — just emit events for all unsent participants in DB |
| `--rescrape` | off | Re-scrape hackathons already scraped in a previous run |

### How it works

```
Phase 1: Discover hackathons
  GET /api/hackathons?status[]=open → paginated JSON listing

Phase 2: Per hackathon
  2a. Fast scan — scrape all participant pages (no enrichment, ~1 req per 20 participants)
  2b. Upsert into SQLite → detect delta (new participants not previously in DB)
  2c. Email-enrich delta only — GitHub API + link walking (skipped with --no-email)
  2d. Emit Customer.io events for unemitted participants (only with --emit-events)
```

### Delta logic

On subsequent runs, the fast scan re-fetches participant lists but only new participants
(not previously in SQLite) get the expensive email enrichment. Already-emitted participants
are never re-emitted. This makes re-runs fast and safe to repeat.

### Common workflows

```bash
# Initial bulk scrape (no events yet)
uv run devpost-harvest --pages 5

# Emit all unsent events from the DB (no scraping, no JWT needed)
uv run devpost-harvest --emit-unsent

# Quick delta check on first hackathon only
uv run devpost-harvest --hackathons 1 --rescrape --emit-events

# Re-scan all hackathons for new participants, enrich + emit
uv run devpost-harvest --rescrape --emit-events

# Include ended hackathons
uv run devpost-harvest --status open --status ended

# Fast delta scan (skip email enrichment for new participants too)
uv run devpost-harvest --rescrape --no-email
```

### SQLite schema

The database (`devpost_harvest.db`) has two tables:

- **`hackathons`** — id, url, title, org, state, dates, registrations, prize, themes.
  `last_scraped_at` is set after participants are scraped.
- **`participants`** — (hackathon_url, username) primary key, enrichment fields,
  `first_seen_at`, `last_seen_at`, `event_emitted_at`.

### Customer.io events

Event name: `devpost_hackathon`. Uses participant email as the Customer.io user ID.

Event data: hackathon_url, hackathon_title, username, name, specialty, profile_url, github_url, linkedin_url.

Email templates in `emails/` use `{{customer.first_name}}` and `{{event.*}}` Liquid variables.

---

## Development

```bash
uv run python -m devpost_scraper.cli "ai agents" --output out.csv
```
