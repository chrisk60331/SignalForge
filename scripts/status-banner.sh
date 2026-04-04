#!/usr/bin/env bash
# status-banner.sh — ASCII banner + version + DB path panel
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/status-lib.sh"
_sf_init_db "${1:-}"

SF_VERSION=$(grep '^version' "${PROJECT_DIR}/pyproject.toml" 2>/dev/null \
  | head -1 | sed 's/version *= *"\(.*\)"/\1/')

cat <<'BANNER'

                                                                   _____ _                   ________
                                                                  / ___/(_)___ _____  ____ _/ / ____/___  _________ ____
                                                                  \__ \/ / __ `/ __ \/ __ `/ / /_  / __ \/ ___/ __ `/ _ \
                                                                 ___/ / / /_/ / / / / /_/ / / __/ / /_/ / /  / /_/ /  __/
                                                                /____/_/\__, /_/ /_/\__,_/_/_/    \____/_/   \__, /\___/
                                                                       /____/                               /____/
BANNER

echo ""
printf "    %-170s %-6s \n" "DB: ${DB}" "v${SF_VERSION:-?}"
