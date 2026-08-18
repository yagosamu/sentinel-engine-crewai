#!/usr/bin/env bash
# eval_ac3.sh — AC-3 gate: source -> gold freshness <= 5 min.
#
# AC-3: a new order in the SOURCE becomes queryable in gold within 5 minutes.
# This eval drives the C8 freshness probe (platform.probe.cli), which injects a
# uniquely-identifiable beacon order into Postgres, runs C2 ingest + dbt build,
# and times how long until the row is visible in gold.gold_orders_obt. The MEDIAN
# end-to-end lag is the AC-3 statistic; the probe restores a clean baseline.
#
# The probe CLI already speaks the exact exit-code contract this eval needs
# (0 PASS / 1 FAIL / 2 ERROR / 77 SKIP), so this wrapper runs it in --ci mode
# (single-shot, JSON), surfaces the measured lag, and propagates its verdict.
#
#   ./eval_ac3.sh           # single-shot CI (1 sample)
#   ./eval_ac3.sh 3         # median of 3 samples (slower, more robust)
#
# Exit: 0 PASS | 1 FAIL | 2 ERROR | 77 SKIP (Postgres down).

set -o pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

SAMPLES="${1:-1}"
BUDGET_S=300

eval_header "AC-3 — Freshness source->gold median lag <= ${BUDGET_S}s (samples=${SAMPLES})"
eval_info "target: a clean beacon order reaches gold within the 5-minute budget"

# Preflight: the probe mutates the source and runs the full pipeline; no DB ->
# unmeasurable. (The probe CLI also SKIPs on its own, but checking here gives a
# clean, uniform message and avoids spawning the run at all.)
if ! db_reachable; then
  eval_skip "AC-3 unmeasurable: source Postgres is unreachable. Start it with 'make up' (then 'make seed') and re-run."
  exit "${EVAL_SKIP}"
fi
if ! gold_ready; then
  eval_skip "AC-3 unmeasurable: gold layer not materialized. Run 'make dbt-build' (after up/seed/ingest-once) and re-run."
  exit "${EVAL_SKIP}"
fi

# Single sample -> use the probe's purpose-built --ci mode (forces 1 sample +
# JSON). Multiple -> pass --samples N --json. Either way we capture JSON to parse
# the measured lag, while the probe's exit code remains the authoritative gate.
if [[ "${SAMPLES}" == "1" ]]; then
  RAW_JSON="$("${PYEXEC[@]}" -m platform.probe.cli --ci 2>/tmp/eval_ac3.stderr)"
else
  RAW_JSON="$("${PYEXEC[@]}" -m platform.probe.cli --samples "${SAMPLES}" --json 2>/tmp/eval_ac3.stderr)"
fi
PROBE_RC=$?

# Honour the probe's own exit-code contract first.
if [[ ${PROBE_RC} -eq ${EVAL_SKIP} ]]; then
  eval_skip "AC-3: probe reported SKIP — $(tr '\n' ' ' </tmp/eval_ac3.stderr)"
  exit "${EVAL_SKIP}"
fi
if [[ ${PROBE_RC} -eq ${EVAL_ERROR} ]]; then
  eval_err "AC-3: probe reported a precondition/pipeline error:"
  sed 's/^/      /' /tmp/eval_ac3.stderr >&2
  exit "${EVAL_ERROR}"
fi

# Surface the measured numbers from the probe's JSON report.
PARSED="$(
  PARSE_JSON="${RAW_JSON}" "${PYEXEC[@]}" - <<'PY'
import json, os
try:
    r = json.loads(os.environ["PARSE_JSON"])
except Exception as exc:  # noqa: BLE001
    print(f"PARSE_ERROR {exc}")
else:
    def g(*keys, default="?"):
        for k in keys:
            if k in r and r[k] is not None:
                return r[k]
        return default
    print("MEDIAN", g("median_lag_s", "median_s", "median"))
    print("MAX",    g("max_lag_s", "max_s", "max"))
    print("BUDGET", g("budget_s", default=300))
    print("VERDICT", g("verdict", "summary", default=""))
PY
)"

if grep -q '^PARSE_ERROR' <<<"${PARSED}"; then
  # The probe already exited 0/1; trust its code but note we could not parse.
  eval_info "(could not parse probe JSON; relying on probe exit code ${PROBE_RC})"
else
  MEDIAN="$(sed -n 's/^MEDIAN //p' <<<"${PARSED}")"
  MAXLAG="$(sed -n 's/^MAX //p' <<<"${PARSED}")"
  BUDGET="$(sed -n 's/^BUDGET //p' <<<"${PARSED}")"
  eval_info "measured median lag: ${MEDIAN}s   max: ${MAXLAG}s   budget: ${BUDGET}s"
fi

if [[ ${PROBE_RC} -eq 0 ]]; then
  eval_pass "AC-3: median source->gold lag within the ${BUDGET_S}s (5 min) budget."
  exit "${EVAL_PASS}"
fi

eval_fail "AC-3: median source->gold lag exceeds the ${BUDGET_S}s budget (or beacon never reached gold)."
exit "${EVAL_FAIL}"
