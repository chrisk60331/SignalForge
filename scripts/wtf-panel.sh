#!/usr/bin/env bash
# WTF dashboard panel wrapper — resolves project paths from script location.
# Called by wtf.yml CmdRunner modules: wtf-panel.sh <section>
#
# Sections: hackathons | forks | search | devto | hn | rb2b | summary | cio | sf

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Ensure uv and brew-installed tools are on PATH for WTF's restricted env
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

DB="${PROJECT_DIR}/devpost_harvest.db"
SECTION="${1:-all}"

exec bash "${SCRIPT_DIR}/status.sh" "$DB" --section "$SECTION"
