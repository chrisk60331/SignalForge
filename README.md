# Devpost Scraper

CLI for extracting Devpost project data with a Backboard assistant that can call a Devpost MCP tool server and export structured results to CSV.

## Requirements

- Python 3.11+
- `uv`
- Node.js / `npx` available on your machine
- A Backboard API key

## Environment

Create a `.env` file from `.env.example` and set:

- `BACKBOARD_API_KEY`
- `BACKBOARD_MODEL` (optional)
- `DEVPOST_ASSISTANT_NAME` (optional)

## MCP server

This project is designed to use a Devpost MCP server with this configuration:

```json
{
  "mcpServers": {
    "devpost": {
      "command": "npx",
      "args": ["devpost-mcp-server"]
    }
  }
}
```

## Install

```bash
uv sync
```

## Run

```bash
uv run devpost-scraper "ai agents" --output ai_agents.csv
uv run devpost-scraper "developer tools" "climate tech" --output results.csv
```

You can also use the startup script:

```bash
./start.sh "ai agents" --output ai_agents.csv
```

## What it does

1. Creates or reuses a Backboard assistant configured for Devpost extraction.
2. Creates a thread for the run.
3. Sends a prompt that asks the assistant to use the Devpost MCP toolset.
4. Handles tool-calling loops until the assistant returns completed structured content.
5. Parses the structured JSON result.
6. Writes the extracted rows to CSV.

## Expected output shape

Each extracted row should contain fields like:

- `search_term`
- `project_title`
- `tagline`
- `project_url`
- `hackathon_name`
- `hackathon_url`
- `summary`
- `built_with`
- `prizes`
- `submission_date`
- `team_size`

## Notes

- The CLI is intentionally API-heavy and UI-free.
- The Backboard assistant must have access to the Devpost MCP tools in the environment where it runs.
- If your Backboard account or environment requires additional tool registration, wire that into the assistant creation flow in the client module.

## Development

```bash
uv run python -m devpost_scraper.cli "ai agents" --output out.csv
```
