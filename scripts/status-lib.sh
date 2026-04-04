#!/usr/bin/env bash
# status-lib.sh — shared helpers for status-*.sh section scripts.
# Source this file; do NOT execute it directly.
#
# After sourcing, call:
#   _sf_init_db "$1"   (pass "$DB_ARG" or the first positional arg)
#
# Exported helpers:
#   _sqlite <sql...>   — retry-safe, read-only sqlite3 call
#   header <text>      — section header
#   row <key> <val>    — key-value row
#   sep                — dotted separator
#   comma <n>          — thousands-formatted integer
#   color_days <n>     — prints n in green (0), yellow (1), or red (>1)
#   color_status <s>   — prints status string with ANSI colour

# ── DB resolution ─────────────────────────────────────────────────────────────
# Each script sources this lib and calls _sf_init_db with the user-supplied path.
# SCRIPT_DIR / PROJECT_DIR are set by the sourcing script (or here as fallback).
: "${SCRIPT_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
: "${PROJECT_DIR:=$(cd "${SCRIPT_DIR}/.." && pwd)}"
: "${DB:=${PROJECT_DIR}/devpost_harvest.db}"

_sf_init_db() {
  local arg="${1:-}"
  if [[ -n "$arg" && "$arg" != --* ]]; then
    DB="$arg"
  fi
  if [[ ! -f "$DB" ]]; then
    echo "Database not found: $DB"
    echo "Run signalforge-harvest first, or pass a path as the first argument."
    exit 1
  fi
}

# ── sqlite wrapper ─────────────────────────────────────────────────────────────
MAX_RETRIES=5
_sqlite() {
  local attempt=0 out rc
  while (( attempt < MAX_RETRIES )); do
    out=$(sqlite3 --readonly -cmd ".timeout 5000" "$DB" "$@" 2>&1)
    rc=$?
    if [[ $rc -eq 0 ]]; then echo "$out"; return 0; fi
    if echo "$out" | grep -qi "database is locked"; then
      (( attempt++ )); sleep 0.3
    else
      echo "$out" >&2; return $rc
    fi
  done
  echo "  ⚠  DB still locked after ${MAX_RETRIES} retries — will refresh next tick" >&2
  return 1
}

# ── Formatting helpers ────────────────────────────────────────────────────────
header() { printf "\n  %s\n  %s\n" "$1" "$(printf -- '-%.0s' {1..54})"; }
row()    { printf "    %-32s  %s\n" "$1" "$2"; }
sep()    { printf "    %s\n" "$(printf '.%.0s' {1..52})"; }

comma() {
  awk -v n="${1:-0}" 'BEGIN {
    r=""
    while (n > 999) { r = "," sprintf("%03d", n % 1000) r; n = int(n / 1000) }
    print n r
  }'
}

color_days() {
  local n="${1:-0}"
  if (( n > 1 )); then
    printf "\033[31m%-6s\033[0m" "$n"
  elif (( n == 0 )); then
    printf "\033[32m%-6s\033[0m" "$n"
  else
    printf "\033[33m%-6s\033[0m" "$n"
  fi
}

color_status() {
  case "$1" in
    running)     printf "\033[33m%-8s\033[0m" "$1" ;;
    done)        printf "\033[32m%-8s\033[0m" "$1" ;;
    failed)      printf "\033[31m%-8s\033[0m" "$1" ;;
    interrupted) printf "\033[35m%-8s\033[0m" "$1" ;;
    *)           printf "%s" "$1" ;;
  esac
}
