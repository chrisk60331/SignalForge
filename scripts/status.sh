#!/usr/bin/env bash
# SignalForge — harvest status dashboard (dispatcher)
# Usage: ./scripts/status.sh [path/to/devpost_harvest.db] [--section <name>]
#
# Sections: banner | hackathons | hn | forks | search | devto | rb2b |
#           runs | freshness | summary | git | all (default)
#
# WTF dashboard: wtf -c scripts/wtf.yml

set -uo pipefail
trap 'echo ""; exit 1' INT TERM SIGINT SIGTERM

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Parse args ────────────────────────────────────────────────────────────────
DB="${SCRIPT_DIR}/../devpost_harvest.db"
SECTION="all"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --section)   SECTION="$2"; shift 2 ;;
    --section=*) SECTION="${1#*=}"; shift ;;
    *)           DB="$1"; shift ;;
  esac
done

_run() {
  bash "${SCRIPT_DIR}/status-${1}.sh" "$DB"
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
case "$SECTION" in
  all)
    _run banner
    _run hackathons
    _run hn
    _run forks
    _run search
    _run devto
    _run rb2b
    _run runs
    _run freshness
    _run summary
    _run git
    ;;
  banner|hackathons|hn|forks|search|devto|rb2b|runs|freshness|summary|git)
    _run "$SECTION"
    ;;
  *)
    echo "Unknown section: '$SECTION'" >&2
    echo "Valid: banner hackathons hn forks search devto rb2b runs freshness summary git all" >&2
    exit 1
    ;;
esac
