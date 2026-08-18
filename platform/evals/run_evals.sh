#!/usr/bin/env bash
# run_evals.sh — run every acceptance-criteria eval and print a verdict table.
#
# Runs, in order:
#   AC-1  eval_ac1.sh             (peak isolation; scaled-down 'smoke' by default)
#   AC-2  eval_ac2.sh             (gold query p95 <= 5s)
#   AC-3  eval_ac3.sh             (source->gold freshness <= 5min)
#   DEFECT eval_defect_survival.sh (inject -> caught in *_rejects, absent from gold)
#
# Each child eval owns its own exit-code contract (0 PASS / 1 FAIL / 2 ERROR /
# 77 SKIP). This runner records each, prints a summary table, and returns:
#   0  if every eval that RAN passed (skips are allowed, not counted as failures),
#   1  if any eval FAILED or ERRORED.
#
# Flags:
#   --ac1-profile <smoke|full>   profile for AC-1 (default: smoke / scaled-down)
#   --ac2-reps    <N>            reps per query for AC-2 (default: 30)
#   --ac3-samples <N>            samples for AC-3 (default: 1)
#   --only <ac1,ac2,ac3,defect>  run only the named evals (comma-separated)
#
# Example:
#   ./run_evals.sh
#   ./run_evals.sh --ac1-profile full --ac3-samples 3
#   ./run_evals.sh --only ac2,ac3

set -o pipefail
EVALS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${EVALS_DIR}/_common.sh"

AC1_PROFILE="smoke"
AC2_REPS="30"
AC3_SAMPLES="1"
ONLY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ac1-profile) AC1_PROFILE="$2"; shift 2 ;;
    --ac2-reps)    AC2_REPS="$2";    shift 2 ;;
    --ac3-samples) AC3_SAMPLES="$2"; shift 2 ;;
    --only)        ONLY=",$2,";      shift 2 ;;
    -h|--help)
      sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) eval_err "unknown flag: $1"; exit 2 ;;
  esac
done

wants() { [[ -z "${ONLY}" || "${ONLY}" == *",$1,"* ]]; }

# Ordered eval registry: id | label | command...
declare -a IDS LABELS
declare -A STATUS RC

record() {
  local id="$1" rc="$2"
  RC[$id]="${rc}"
  case "${rc}" in
    0)  STATUS[$id]="PASS" ;;
    1)  STATUS[$id]="FAIL" ;;
    77) STATUS[$id]="SKIP" ;;
    *)  STATUS[$id]="ERROR" ;;
  esac
}

run_one() {
  local id="$1" label="$2"; shift 2
  IDS+=("${id}"); LABELS+=("${label}")
  if ! wants "${id}"; then
    record "${id}" 70   # sentinel: not selected
    STATUS[$id]="—"
    return
  fi
  "$@"
  record "${id}" "$?"
}

START_TS="$(date +%s)"

run_one ac1    "AC-1 peak isolation"        bash "${EVALS_DIR}/eval_ac1.sh" "${AC1_PROFILE}"
run_one ac2    "AC-2 gold p95 <= 5s"        bash "${EVALS_DIR}/eval_ac2.sh" "${AC2_REPS}"
run_one ac3    "AC-3 freshness <= 5min"     bash "${EVALS_DIR}/eval_ac3.sh" "${AC3_SAMPLES}"
run_one defect "DEFECT survival (U3)"       bash "${EVALS_DIR}/eval_defect_survival.sh"

ELAPSED=$(( $(date +%s) - START_TS ))

# --- verdict table -----------------------------------------------------------
printf '\n'
printf '%s%s%s\n' "${C_BOLD}" "=====================================================================" "${C_RESET}"
printf '%s  EVAL SUMMARY%s   (elapsed %ss)\n' "${C_BOLD}" "${C_RESET}" "${ELAPSED}"
printf '%s%s%s\n' "${C_BOLD}" "=====================================================================" "${C_RESET}"
printf '  %-8s  %-30s  %s\n' "ID" "CRITERION" "VERDICT"
printf '  %-8s  %-30s  %s\n' "--------" "------------------------------" "-------"

color_for() {
  case "$1" in
    PASS)  printf '%s' "${C_GREEN}${C_BOLD}" ;;
    FAIL|ERROR) printf '%s' "${C_RED}${C_BOLD}" ;;
    SKIP)  printf '%s' "${C_YELLOW}${C_BOLD}" ;;
    *)     printf '%s' "${C_DIM}" ;;
  esac
}

FAILED=0; SKIPPED=0; PASSED=0
for i in "${!IDS[@]}"; do
  id="${IDS[$i]}"; label="${LABELS[$i]}"; st="${STATUS[$id]}"
  printf '  %-8s  %-30s  %s%s%s\n' "${id}" "${label}" "$(color_for "${st}")" "${st}" "${C_RESET}"
  case "${st}" in
    PASS) PASSED=$((PASSED+1)) ;;
    SKIP) SKIPPED=$((SKIPPED+1)) ;;
    FAIL|ERROR) FAILED=$((FAILED+1)) ;;
  esac
done
printf '%s%s%s\n' "${C_BOLD}" "=====================================================================" "${C_RESET}"
printf '  totals: %s%d PASS%s  %s%d FAIL/ERROR%s  %s%d SKIP%s\n' \
  "${C_GREEN}" "${PASSED}" "${C_RESET}" \
  "${C_RED}" "${FAILED}" "${C_RESET}" \
  "${C_YELLOW}" "${SKIPPED}" "${C_RESET}"

if [[ ${FAILED} -gt 0 ]]; then
  printf '%sOVERALL: FAIL%s — at least one criterion was measured and breached (or errored).\n' "${C_RED}${C_BOLD}" "${C_RESET}"
  exit 1
fi
if [[ ${PASSED} -eq 0 ]]; then
  printf '%sOVERALL: INCONCLUSIVE%s — nothing was measured (all skipped). Bring up infra and re-run.\n' "${C_YELLOW}${C_BOLD}" "${C_RESET}"
  exit 0
fi
printf '%sOVERALL: PASS%s — every eval that ran met its criterion.\n' "${C_GREEN}${C_BOLD}" "${C_RESET}"
exit 0
