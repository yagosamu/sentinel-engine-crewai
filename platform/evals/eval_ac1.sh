#!/usr/bin/env bash
# eval_ac1.sh — AC-1 gate: peak isolation under load.
#
# AC-1: at peak order volume (75k/day-equivalent), the analytics path causes
# ZERO lock-waits on the transactional (OLTP) path. This eval drives the C4h
# peak-load harness (platform.harness.cli), which applies a saturating OLTP
# insert load alongside concurrent analytics readers and audits pg_blocking_pids
# for any analytics-attributable lock-wait on an oltp_writer session.
#
# SCALED-DOWN OK: defaults to the `smoke` profile (same attribution code path,
# ~4s window, lower floor) so it runs fast in CI. Pass `full` for the real
# 75k/day-equiv, 60s gate.
#
#   ./eval_ac1.sh            # smoke (scaled-down)
#   ./eval_ac1.sh full       # full 75k/day-equiv gate
#
# Exit: 0 PASS | 1 FAIL | 2 ERROR | 77 SKIP (Postgres down).

set -o pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

PROFILE="${1:-smoke}"

eval_header "AC-1 — Peak isolation (profile=${PROFILE})"
eval_info "target: 0 analytics-attributable lock-waits on the OLTP path under peak load"

# Preflight: the harness drives REAL Postgres sessions; no DB -> unmeasurable.
if ! db_reachable; then
  eval_skip "AC-1 unmeasurable: source Postgres is unreachable. Start it with 'make up' (then 'make seed') and re-run."
  exit "${EVAL_SKIP}"
fi

# Run the harness in JSON mode so we can extract the measured numbers. Capture
# the harness exit code (its own gate) separately from the JSON parse.
RAW_JSON="$("${PYEXEC[@]}" -m platform.harness.cli --profile "${PROFILE}" --json 2>/tmp/eval_ac1.stderr)"
HARNESS_RC=$?

if [[ ${HARNESS_RC} -eq 2 ]]; then
  eval_err "AC-1 precondition error from the harness:"
  sed 's/^/      /' /tmp/eval_ac1.stderr >&2
  exit "${EVAL_ERROR}"
fi

# Parse the structured report for the headline metrics + final verdict.
PARSED="$(
  PARSE_JSON="${RAW_JSON}" "${PYEXEC[@]}" - <<'PY'
import json, os, sys
try:
    r = json.loads(os.environ["PARSE_JSON"])
except Exception as exc:  # noqa: BLE001
    print(f"PARSE_ERROR {exc}")
    sys.exit(0)
print("PASSED", r.get("passed"))
print("FLOOR_MET", r.get("load_floor_met"))
print("ACHIEVED", f"{r.get('achieved_orders_per_day', 0):,.0f}")
print("FLOOR", f"{r.get('min_orders_per_day_floor', 0):,}")
print("COMMITS", r.get("orders_committed"))
print("LOCKWAITS", r.get("analytics_attributable_lock_waits"))
print("THRESH", r.get("max_allowed_analytics_lock_waits"))
print("SAMPLES", r.get("samples_taken"))
print("OLTP_P95", (r.get("oltp") or {}).get("p95_ms"))
print("ANA_P95", (r.get("analytics") or {}).get("p95_ms"))
print("VERDICT", r.get("verdict"))
PY
)"

if grep -q '^PARSE_ERROR' <<<"${PARSED}"; then
  eval_err "could not parse harness JSON output:"
  printf '%s\n' "${RAW_JSON}" | sed 's/^/      /' >&2
  exit "${EVAL_ERROR}"
fi

get() { sed -n "s/^$1 //p" <<<"${PARSED}"; }
PASSED="$(get PASSED)"; FLOOR_MET="$(get FLOOR_MET)"
ACHIEVED="$(get ACHIEVED)"; FLOOR="$(get FLOOR)"; COMMITS="$(get COMMITS)"
LOCKWAITS="$(get LOCKWAITS)"; THRESH="$(get THRESH)"; SAMPLES="$(get SAMPLES)"
OLTP_P95="$(get OLTP_P95)"; ANA_P95="$(get ANA_P95)"; VERDICT="$(get VERDICT)"

eval_info "achieved load:      ${ACHIEVED} orders/day-equiv (floor ${FLOOR}; ${COMMITS} commits)  floor_met=${FLOOR_MET}"
eval_info "lock-wait samples:  ${SAMPLES}"
eval_info "analytics-attributable lock-waits: ${LOCKWAITS}  (threshold ${THRESH})"
eval_info "OLTP commit p95: ${OLTP_P95} ms   analytics query p95: ${ANA_P95} ms"

# Distinguish the three harness outcomes precisely:
#   passed=True            -> PASS (peak load applied AND clean OLTP path)
#   floor not met          -> SKIP-as-inconclusive (not a valid peak test on this box)
#   floor met, contention  -> FAIL (a real isolation breach)
if [[ "${PASSED}" == "True" ]]; then
  eval_pass "AC-1: ${VERDICT}"
  exit "${EVAL_PASS}"
fi

if [[ "${FLOOR_MET}" != "True" ]]; then
  eval_skip "AC-1 INCONCLUSIVE: load floor not met (achieved ${ACHIEVED} < ${FLOOR}); this box did not apply peak load, so the result is not a valid gate. ${VERDICT}"
  exit "${EVAL_SKIP}"
fi

eval_fail "AC-1: ${VERDICT}"
exit "${EVAL_FAIL}"
