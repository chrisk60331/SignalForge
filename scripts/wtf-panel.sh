#!/usr/bin/env bash
# WTF dashboard panel wrapper — resolves project paths from script location.
# Called by wtf.yml CmdRunner modules: wtf-panel.sh <section>
#
# Sections: banner | hackathons | hn | forks | search | devto | rb2b |
#           runs | freshness | summary | git | empty

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Ensure uv and brew-installed tools are on PATH for WTF's restricted env
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

DB="${PROJECT_DIR}/devpost_harvest.db"
SECTION="${1:-all}"

if [[ "$SECTION" == "empty" ]]; then
my_list=( \
 "Scrape More :(" \
 "Scrape Scrape Scrape" \
 "ABS Always Be Scraping" \
 "The Forge Is Hungry" \
 "MOAR!" \
 "Please sir could I have some more?" \
 "More campaigns, more glory!"
 "The Campaign Cauldron is empty – feed me!"
 "Tip: Add campaigns for endless surprises"
 "Hungry for hacks? Add your next campaign!"
 "Nothing to see... unless you add a campaign!"
 )

max() {
    local m=$1
    shift
    for i in "$@"; do
        (( i > m )) && m=$i
    done
    echo "$m"
}

min() {
    local low=$1
    for arg in "$@"; do
        if (( arg < low )); then
            low=$arg
        fi
    done
    echo "$low"
}

# Select a random index
index=$(( RANDOM % ${#my_list[@]} ))
l=0
if [ ${#my_list[$index]} -gt 35  ]; then
  l=0
elif [ ${#my_list[$index]} -gt 30  ]; then
  l=5
elif [ ${#my_list[$index]} -gt 25  ]; then
  l=8
elif [ ${#my_list[$index]} -gt 20  ]; then
  l=9
elif [ ${#my_list[$index]} -gt 15  ]; then
  l=11
elif [ ${#my_list[$index]} -lt 10  ]; then
  l=18
else
  l=14
fi
echo 
  printf "%-${l}s %-9s\n" "" "${my_list[$index]}"
  echo ""
  exit 0
fi

exec bash "${SCRIPT_DIR}/status-${SECTION}.sh" "$DB"
