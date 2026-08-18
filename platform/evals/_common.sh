#!/usr/bin/env bash
# platform/evals/_common.sh — shared helpers for the acceptance-criteria evals.
#
# Sourced (not executed) by every eval_*.sh. Centralises:
#   - repo-root resolution (so an eval works from any cwd),
#   - the canonical Python interpreter (.venv, falling back to `uv run`),
#   - the standard exit-code contract every eval honours,
#   - infra preflight checks (Postgres reachable, DuckDB warehouse + gold present),
#   - uniform PASS / FAIL / SKIP verdict printers.
#
# EXIT-CODE CONTRACT (every eval_*.sh returns one of these):
#   0   PASS         — the acceptance criterion was MEASURED and met.
#   1   FAIL         — the criterion was measured and BREACHED.
#   2   ERROR        — a precondition/pipeline error; the gate could not be run.
#   77  SKIPPED      — required infra is missing; the gate is UNMEASURABLE
#                      (never silently passes). Mirrors the C8 probe's SKIP code.
#
# Nothing here mutates state. Evals that mutate (defect-survival) do their own
# inject/reset and restore a clean baseline themselves (rule R7).

set -o pipefail

# --- exit-code constants -----------------------------------------------------
EVAL_PASS=0
EVAL_FAIL=1
EVAL_ERROR=2
EVAL_SKIP=77

# --- repo root ---------------------------------------------------------------
# This file lives at <repo>/platform/evals/_common.sh, so the repo root is two
# directories up from here. Resolve it independently of the caller's cwd.
_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${_COMMON_DIR}/../.." && pwd)"

# --- python interpreter ------------------------------------------------------
# Prefer the repo's venv (fast, no resolver). Fall back to `uv run python` so the
# evals still work on a machine that only has uv. PYEXEC is an array so callers
# can do: "${PYEXEC[@]}" -m platform.harness.cli ...
if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PYEXEC=("${REPO_ROOT}/.venv/bin/python")
elif command -v uv >/dev/null 2>&1; then
  PYEXEC=(uv run python)
else
  PYEXEC=(python3)
fi

# The top-level package is named `platform` (collides with stdlib); the repo root
# must be on PYTHONPATH for `python -m platform.*` to resolve.
export PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

# --- colours (only when stdout is a TTY) -------------------------------------
if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'; C_GREEN=$'\033[32m'; C_RED=$'\033[31m'
  C_YELLOW=$'\033[33m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
else
  C_RESET=""; C_GREEN=""; C_RED=""; C_YELLOW=""; C_BOLD=""; C_DIM=""
fi

# --- verdict printers --------------------------------------------------------
# Each prints a single labelled line; the eval's exit code is the real contract,
# these are for humans reading the log.
eval_pass() { printf '%s[ PASS ]%s %s\n' "${C_GREEN}${C_BOLD}" "${C_RESET}" "$*"; }
eval_fail() { printf '%s[ FAIL ]%s %s\n' "${C_RED}${C_BOLD}" "${C_RESET}" "$*"; }
eval_skip() { printf '%s[ SKIP ]%s %s\n' "${C_YELLOW}${C_BOLD}" "${C_RESET}" "$*"; }
eval_err()  { printf '%s[ ERR  ]%s %s\n' "${C_RED}${C_BOLD}" "${C_RESET}" "$*"; }
eval_info() { printf '%s  ->%s %s\n' "${C_DIM}" "${C_RESET}" "$*"; }

eval_header() {
  printf '\n%s%s%s\n' "${C_BOLD}" "============================================================" "${C_RESET}"
  printf '%s  %s%s\n' "${C_BOLD}" "$*" "${C_RESET}"
  printf '%s%s%s\n'   "${C_BOLD}" "============================================================" "${C_RESET}"
}

# --- preflight: is the source Postgres reachable? ----------------------------
# Returns 0 if a SELECT 1 succeeds within a short timeout, else 1. Uses the repo
# connection helper so it honours the same POSTGRES_* env / .env the pipeline does.
db_reachable() {
  "${PYEXEC[@]}" - <<'PY' 2>/dev/null
import sys
try:
    import psycopg
    from src.db.connection import conninfo
    with psycopg.connect(**conninfo(), connect_timeout=3) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1"); cur.fetchone()
except Exception:
    sys.exit(1)
sys.exit(0)
PY
}

# --- preflight: does the DuckDB warehouse exist with a populated gold schema? -
# Returns 0 when gold.gold_orders_obt is present AND non-empty (the readers' SLO
# target). Opens read-only so it never contends with a writer.
gold_ready() {
  "${PYEXEC[@]}" - <<'PY' 2>/dev/null
import sys
try:
    from platform.warehouse.connection import connect_read_only
    c = connect_read_only()
    try:
        n = c.execute(
            "select count(*) from information_schema.tables "
            "where table_schema='gold' and table_name='gold_orders_obt'"
        ).fetchone()[0]
        if not n:
            sys.exit(1)
        rows = c.execute("select count(*) from gold.gold_orders_obt").fetchone()[0]
        sys.exit(0 if rows > 0 else 1)
    finally:
        c.close()
except Exception:
    sys.exit(1)
PY
}
