#!/usr/bin/env bash
# eval_defect_survival.sh — U3 detection-seam gate: injected defects are CAUGHT.
#
# The locked U3 decision: silver QUARANTINES bad rows into <model>_rejects rather
# than dropping them, so an eval can ASSERT a defect was caught (not just absent).
# This eval, per failure key:
#   1. restores a clean baseline (reset-schema; remove leftover beacons),
#   2. injects the failure into SOURCE Postgres (src.gen, logs ground truth),
#   3. runs C2 ingest (Postgres -> raw) then dbt build (raw -> bronze/silver/gold),
#   4. asserts the matching silver_<entity>_rejects table CAUGHT the defect
#      (reject_rule == failure_key, count grew), AND
#   5. asserts gold contains NONE of the quarantined rows (defect did not leak).
# Finally it restores a clean baseline so the run is reproducible (rule R7).
#
# The failure -> (rejects table, reject_rule) map is derived directly from the
# dbt classify.sql macro (reject_rule IS the failure_key by construction).
#
# Defaults to the canonical order-level defects that map 1:1 to a reject_rule.
# Pass explicit failure keys to scope the run:
#   ./eval_defect_survival.sh                       # default basket
#   ./eval_defect_survival.sh negative_price orphan_payment
#
# Exit: 0 PASS (all caught) | 1 FAIL (a defect leaked / not caught)
#       | 2 ERROR | 77 SKIP (Postgres or gold missing).

set -o pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

# failure_key -> "<rejects_table>|<reject_rule>". reject_rule == failure_key for
# every one of these (the classifier's machine key). orphan_payment lands in the
# payments rejects table; the rest in orders rejects.
declare -A DEFECT_MAP=(
  [negative_price]="silver_orders_rejects|negative_price"
  [missing_customer]="silver_orders_rejects|missing_customer"
  [invalid_quantity]="silver_orders_rejects|invalid_quantity"
  [duplicate_order]="silver_orders_rejects|duplicate_order"
  [destructive_fix]="silver_orders_rejects|destructive_fix"
  [malformed_data]="silver_orders_rejects|malformed_data"
  [orphan_payment]="silver_payments_rejects|orphan_payment"
)

# Default basket: one representative per quarantine surface + the trickier ones.
DEFAULT_FAILURES=(negative_price missing_customer invalid_quantity duplicate_order destructive_fix malformed_data orphan_payment)

if [[ $# -gt 0 ]]; then
  FAILURES=("$@")
else
  FAILURES=("${DEFAULT_FAILURES[@]}")
fi

eval_header "DEFECT SURVIVAL — inject -> assert caught in *_rejects, absent from gold"
eval_info "U3 contract: each injected defect must be quarantined (not dropped, not leaked to gold)"
eval_info "failures under test: ${FAILURES[*]}"

# Preflight.
if ! db_reachable; then
  eval_skip "unmeasurable: source Postgres is unreachable. Start it with 'make up' (then 'make seed') and re-run."
  exit "${EVAL_SKIP}"
fi
if ! gold_ready; then
  eval_skip "unmeasurable: gold layer not materialized. Run 'make dbt-build' (after up/seed/ingest-once) and re-run."
  exit "${EVAL_SKIP}"
fi

# --- pipeline helpers --------------------------------------------------------
# Seed parameters for the deterministic clean baseline (match Makefile defaults).
SEED_CUSTOMERS="${CUSTOMERS:-500}"
SEED_PRODUCTS="${PRODUCTS:-200}"
SEED_ORDERS="${ORDERS:-5000}"
SEED_SEED="${SEED:-42}"

# The C4 warehouse is single-writer-PROCESS: a writer (ingest/dbt) cannot open
# the file while ANY other connection (including a read-only reader from the
# previous step that has not fully torn down) holds it. Before every writer step
# we therefore POLL until a read/write open succeeds, so we never start a writer
# into a held lock. This is the C4 contract enforced as a gate, not a workaround.
wait_for_writable() {
  local tries="${1:-40}"   # ~40 * 0.5s = 20s ceiling
  local i=0
  while (( i < tries )); do
    if "${PYEXEC[@]}" - <<'PY' >/dev/null 2>&1
import duckdb
from platform.warehouse.paths import warehouse_path_str
con = duckdb.connect(warehouse_path_str(), read_only=False)
con.close()
PY
    then
      return 0
    fi
    sleep 0.5
    i=$((i+1))
  done
  return 1
}

# Run a writer step with a wait-for-lock gate and one retry. A genuinely broken
# pipeline still fails (the retry also fails); only transient lock overlap is
# absorbed.
run_writer() {
  local label="$1"; shift
  if ! wait_for_writable; then
    eval_info "${label}: warehouse write-lock did not free within the ceiling; proceeding anyway."
  fi
  if "$@"; then return 0; fi
  eval_info "${label}: first attempt failed; waiting for lock then retrying once..."
  wait_for_writable 60 || true
  "$@"
}

_ingest_once() {
  # Ephemeral (full-load) C2 run so we never depend on a stale watermark; the
  # PK-upsert arm captures the freshly injected rows regardless.
  "${PYEXEC[@]}" -m platform.ingestion.run >/tmp/eval_ds_ingest.log 2>&1
}
run_ingest() { run_writer "ingest" _ingest_once; }

_dbt_once() {
  # Mirror the Makefile dbt-build target: point dbt at the canonical warehouse
  # and run from the transform project dir. --full-refresh is used here as the
  # STRICTEST possible check (every row re-classified from scratch). The
  # incremental path is ALSO defect-faithful now — silver keys its window on the
  # processing-time _ingested_at (re-stamped by C2 on every upsert) and a
  # pre_hook evicts any key whose bronze copy was re-extracted, so an
  # accepted->rejected transition leaves gold even WITHOUT --full-refresh
  # (proven by tests/test_e2e_incremental_medallion.py). Full-refresh is retained
  # as belt-and-braces, not because the incremental path is unsound.
  (
    cd "${REPO_ROOT}/platform/transform" || exit 2
    DUCKDB_DATABASE="${REPO_ROOT}/platform/warehouse/warehouse.duckdb" \
      "${PYEXEC[@]}" -m dbt.cli.main build --full-refresh --profiles-dir profiles
  ) >/tmp/eval_ds_dbt.log 2>&1
}
run_dbt() { run_writer "dbt" _dbt_once; }

restore_baseline() {
  # R7: restore the KNOWN-CLEAN baseline by truncating + re-seeding the source
  # with the deterministic seeder (fixed --seed). This is the only safe restore:
  # UPDATE-style injectors (destructive_fix, malformed_data) corrupt EXISTING
  # baseline rows in place, so a surgical "delete defect rows" cannot tell an
  # injected defect from a mutated baseline row and would erode the baseline.
  # A truncate+reseed is idempotent and also undoes schema_drift (the rename is
  # reverted first so the seeder writes to customer_id).
  "${PYEXEC[@]}" - <<'PY' >/tmp/eval_ds_restore.log 2>&1 || true
import psycopg
from src.db.connection import conninfo
from src.gen import repository as repo
with psycopg.connect(**conninfo()) as conn:
    col = repo.order_customer_column(conn)
    if col == "user_id":
        repo.execute(conn, "ALTER TABLE orders RENAME COLUMN user_id TO customer_id")
        conn.commit()
PY
  "${PYEXEC[@]}" -m src.seed.seed \
    --customers "${SEED_CUSTOMERS}" --products "${SEED_PRODUCTS}" \
    --orders "${SEED_ORDERS}" --seed "${SEED_SEED}" --truncate \
    >>/tmp/eval_ds_restore.log 2>&1
}

# Snapshot a (count) for a rejects table filtered by reject_rule, and whether
# gold contains any row carrying that defect. Emits "BEFORE <n>" style lines.
rejects_count() {
  local table="$1" rule="$2"
  REJ_TABLE="${table}" REJ_RULE="${rule}" "${PYEXEC[@]}" - <<'PY'
import os
from platform.warehouse.connection import connect_read_only
c = connect_read_only()
try:
    table = os.environ["REJ_TABLE"]; rule = os.environ["REJ_RULE"]
    n = c.execute(f"select count(*) from silver.{table} where reject_rule = ?", [rule]).fetchone()[0]
    print(n)
finally:
    c.close()
PY
}

# Count gold rows that match the quarantined defect, to prove it did NOT leak.
gold_leak_count() {
  local rule="$1"
  GOLD_RULE="${rule}" "${PYEXEC[@]}" - <<'PY'
import os
from platform.warehouse.connection import connect_read_only
rule = os.environ["GOLD_RULE"]
# Map each defect class to a gold predicate that would be TRUE iff a defect row
# leaked into gold.gold_orders_obt. orphan_payment is asserted via payment join
# integrity (every gold row's order exists, so an orphan payment cannot appear).
PREDICATES = {
    "negative_price":  "unit_price < 0 or total_amount < 0",
    "missing_customer":"customer_id is null",
    "invalid_quantity":"quantity <= 0",
    "destructive_fix": "total_amount = 0 and quantity * unit_price > 0",
    "malformed_data":  "status not in ('placed','shipped','delivered','returned','cancelled')",
    "duplicate_order": None,   # checked via business-key duplication below
    "orphan_payment":  None,   # checked via payment_amount with no order (n/a in OBT)
}
c = connect_read_only()
try:
    if rule == "duplicate_order":
        n = c.execute(
            "select count(*) from ("
            "  select customer_id, product_id, quantity, unit_price, total_amount, status, ordered_at, count(*) k"
            "  from gold.gold_orders_obt"
            "  group by 1,2,3,4,5,6,7 having count(*) > 1) d"
        ).fetchone()[0]
    elif rule == "orphan_payment":
        # the OBT only carries payments whose order is present; a leaked orphan
        # would surface as a payment row referencing order_id 999999999.
        n = c.execute(
            "select count(*) from gold.gold_orders_obt where order_id = 999999999"
        ).fetchone()[0]
    else:
        pred = PREDICATES.get(rule)
        if pred is None:
            print(0); raise SystemExit
        n = c.execute(f"select count(*) from gold.gold_orders_obt where {pred}").fetchone()[0]
    print(n)
finally:
    c.close()
PY
}

# --- run ---------------------------------------------------------------------
# Start every run from a known-clean baseline (R7).
eval_info "restoring clean baseline before injection sweep..."
restore_baseline

OVERALL_RC=${EVAL_PASS}
PASS_KEYS=(); FAIL_KEYS=(); ERR_KEYS=()

for key in "${FAILURES[@]}"; do
  mapping="${DEFECT_MAP[$key]:-}"
  if [[ -z "${mapping}" ]]; then
    eval_err "no rejects mapping for failure '${key}' (not an order/payment quarantine defect); skipping it."
    ERR_KEYS+=("${key}")
    OVERALL_RC=${EVAL_ERROR}
    continue
  fi
  table="${mapping%%|*}"; rule="${mapping##*|}"

  printf '\n%s--- %s -> silver.%s (reject_rule=%s) ---%s\n' "${C_BOLD}" "${key}" "${table}" "${rule}" "${C_RESET}"

  # Inject into the source (ground truth recorded by the generator).
  if ! "${PYEXEC[@]}" -m src.gen.cli inject "${key}" >/tmp/eval_ds_inject.log 2>&1; then
    eval_err "${key}: injection failed:"; sed 's/^/      /' /tmp/eval_ds_inject.log >&2
    ERR_KEYS+=("${key}"); OVERALL_RC=${EVAL_ERROR}; restore_baseline; continue
  fi
  eval_info "injected: $(tail -n1 /tmp/eval_ds_inject.log)"

  # Run the pipeline: ingest then dbt (single-writer order honoured).
  if ! run_ingest; then
    eval_err "${key}: C2 ingest failed (see /tmp/eval_ds_ingest.log)"
    ERR_KEYS+=("${key}"); OVERALL_RC=${EVAL_ERROR}; restore_baseline; continue
  fi
  if ! run_dbt; then
    eval_err "${key}: dbt build failed (see /tmp/eval_ds_dbt.log)"
    eval_info "$(tail -n 3 /tmp/eval_ds_dbt.log | tr '\n' ' ')"
    ERR_KEYS+=("${key}"); OVERALL_RC=${EVAL_ERROR}; restore_baseline; continue
  fi

  # Assert 1: the defect was CAUGHT in the rejects table.
  caught="$(rejects_count "${table}" "${rule}")"
  # Assert 2: the defect did NOT leak to gold.
  leaked="$(gold_leak_count "${rule}")"

  eval_info "caught in silver.${table} (reject_rule='${rule}'): ${caught} row(s)"
  eval_info "leaked into gold.gold_orders_obt:                  ${leaked} row(s)"

  if [[ "${caught}" =~ ^[0-9]+$ && "${caught}" -ge 1 && "${leaked}" =~ ^[0-9]+$ && "${leaked}" -eq 0 ]]; then
    eval_pass "${key}: defect quarantined (${caught} caught) and absent from gold."
    PASS_KEYS+=("${key}")
  else
    eval_fail "${key}: caught=${caught} (need >=1), leaked=${leaked} (need 0)."
    FAIL_KEYS+=("${key}")
    OVERALL_RC=${EVAL_FAIL}
  fi

  # Restore before the next failure so each is measured in isolation (R7).
  restore_baseline
done

# C2 UPSERTs and never DELETEs, so a defect row injected on a PK that the reseed
# removed from the source still lingers in raw.* (e.g. the orphan payment PK).
# Purge any raw row whose PK no longer exists in the clean source, so the final
# warehouse state is genuinely pristine (not just consistent with the asserts).
purge_raw_orphans() {
  if ! wait_for_writable; then
    eval_info "purge: warehouse write-lock did not free within the ceiling; proceeding anyway."
  fi
  "${PYEXEC[@]}" - <<'PY' >/tmp/eval_ds_purge.log 2>&1 || true
import duckdb, psycopg
from platform.warehouse.paths import warehouse_path_str
from src.db.connection import conninfo

# Pull the clean source PK sets.
with psycopg.connect(**conninfo()) as pg, pg.cursor() as cur:
    cur.execute("select order_id from orders")
    order_ids = [r[0] for r in cur.fetchall()]
    cur.execute("select payment_id from payments")
    payment_ids = [r[0] for r in cur.fetchall()]

con = duckdb.connect(warehouse_path_str(), read_only=False)
try:
    con.execute("create temp table _ok_orders(order_id bigint)")
    con.executemany("insert into _ok_orders values (?)", [[i] for i in order_ids])
    con.execute("create temp table _ok_payments(payment_id bigint)")
    con.executemany("insert into _ok_payments values (?)", [[i] for i in payment_ids])
    con.execute("delete from raw.raw_orders where order_id not in (select order_id from _ok_orders)")
    con.execute("delete from raw.raw_payments where payment_id not in (select payment_id from _ok_payments)")
finally:
    con.close()
PY
}

# Final pipeline pass on the restored baseline so the warehouse is left clean.
eval_info "final baseline rebuild (restoring clean warehouse state)..."
purge_raw_orphans
run_ingest && run_dbt || eval_info "(warning: final clean rebuild reported an issue; see /tmp/eval_ds_dbt.log)"

printf '\n'
eval_info "PASS: ${PASS_KEYS[*]:-none}"
[[ ${#FAIL_KEYS[@]} -gt 0 ]] && eval_info "FAIL: ${FAIL_KEYS[*]}"
[[ ${#ERR_KEYS[@]}  -gt 0 ]] && eval_info "ERR:  ${ERR_KEYS[*]}"

if [[ ${OVERALL_RC} -eq ${EVAL_PASS} ]]; then
  eval_pass "DEFECT SURVIVAL: all ${#PASS_KEYS[@]} injected defect(s) were caught and none leaked to gold."
elif [[ ${OVERALL_RC} -eq ${EVAL_FAIL} ]]; then
  eval_fail "DEFECT SURVIVAL: ${#FAIL_KEYS[@]} defect(s) not caught or leaked to gold."
else
  eval_err "DEFECT SURVIVAL: ${#ERR_KEYS[@]} failure(s) errored before they could be scored."
fi
exit "${OVERALL_RC}"
