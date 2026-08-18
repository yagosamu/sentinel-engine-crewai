#!/usr/bin/env bash
# eval_ac2.sh — AC-2 gate: gold query latency p95 <= 5s.
#
# AC-2: analytical queries over the gold OBTs return within a 5-second p95 budget.
# This eval opens the shared DuckDB warehouse READ-ONLY (so it never contends
# with a writer) and times a representative basket of gold queries — full-table
# aggregations, group-bys, top-N, and a join across the two gold models — over N
# repetitions, then reports the aggregate p95 (and per-query p95) against 5000 ms.
#
#   ./eval_ac2.sh           # default 30 reps per query
#   ./eval_ac2.sh 50        # 50 reps per query
#
# Exit: 0 PASS | 1 FAIL | 2 ERROR | 77 SKIP (warehouse/gold missing).

set -o pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

REPS="${1:-30}"
BUDGET_MS=5000

eval_header "AC-2 — Gold query latency p95 <= ${BUDGET_MS} ms (reps=${REPS})"
eval_info "target: aggregate p95 over a basket of gold OBT queries within the 5s budget"

# Preflight: AC-2 is a property of the materialized gold layer. No gold -> skip.
if ! gold_ready; then
  eval_skip "AC-2 unmeasurable: gold.gold_orders_obt is missing or empty. Materialize it with 'make dbt-build' (after up/seed/ingest-once) and re-run."
  exit "${EVAL_SKIP}"
fi

# Drive the measurement in Python (DuckDB timing with high-res perf_counter).
RESULT="$(
  AC2_REPS="${REPS}" AC2_BUDGET_MS="${BUDGET_MS}" "${PYEXEC[@]}" - <<'PY'
import json, os, sys, time
from platform.warehouse.connection import connect_read_only

reps = int(os.environ["AC2_REPS"])
budget_ms = float(os.environ["AC2_BUDGET_MS"])

# A representative basket exercising the two gold OBTs: full scans, group-bys,
# top-N with ordering, a window, and a cross-model join. These mirror the shapes
# C5 / the platform consumer issue against gold.
QUERIES = {
    "full_scan_count":
        "select count(*), sum(total_amount) from gold.gold_orders_obt",
    "revenue_by_category":
        "select product_category, count(*) c, sum(total_amount) rev "
        "from gold.gold_orders_obt group by 1 order by rev desc",
    "revenue_by_country":
        "select customer_country, count(*) c, sum(total_amount) rev "
        "from gold.gold_orders_obt group by 1 order by rev desc",
    "top_products":
        "select product_id, product_name, sum(total_amount) rev "
        "from gold.gold_orders_obt group by 1,2 order by rev desc limit 10",
    "status_breakdown":
        "select status, count(*) c from gold.gold_orders_obt group by 1 order by c desc",
    "daily_revenue_window":
        "select order_date, product_category, gross_revenue, "
        "sum(gross_revenue) over (partition by product_category order by order_date) running "
        "from gold.gold_revenue_daily order by order_date desc limit 100",
    "join_obt_x_daily":
        "select d.order_date, d.product_category, d.gross_revenue, count(o.order_id) orders "
        "from gold.gold_revenue_daily d "
        "left join gold.gold_orders_obt o "
        "  on cast(o.ordered_at as date) = d.order_date "
        " and o.product_category = d.product_category "
        "group by 1,2,3 order by d.order_date desc limit 50",
}

def pct(vals, p):
    if not vals:
        return 0.0
    s = sorted(vals)
    # nearest-rank percentile
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * len(s) + 0.5)) - 1))
    return s[k]

conn = connect_read_only()
all_samples = []
per_query = {}
try:
    # Warm the file cache once per query (not counted) so we measure steady-state
    # query latency, not first-touch disk read.
    for sql in QUERIES.values():
        conn.execute(sql).fetchall()
    for name, sql in QUERIES.items():
        samples = []
        for _ in range(reps):
            t0 = time.perf_counter()
            conn.execute(sql).fetchall()
            samples.append((time.perf_counter() - t0) * 1000.0)
        per_query[name] = {
            "p50_ms": round(pct(samples, 50), 2),
            "p95_ms": round(pct(samples, 95), 2),
            "max_ms": round(max(samples), 2),
        }
        all_samples.extend(samples)
finally:
    conn.close()

agg_p95 = round(pct(all_samples, 95), 2)
agg_p50 = round(pct(all_samples, 50), 2)
agg_max = round(max(all_samples), 2)
passed = agg_p95 <= budget_ms

print(json.dumps({
    "passed": passed,
    "budget_ms": budget_ms,
    "reps_per_query": reps,
    "n_queries": len(QUERIES),
    "total_samples": len(all_samples),
    "agg_p50_ms": agg_p50,
    "agg_p95_ms": agg_p95,
    "agg_max_ms": agg_max,
    "per_query": per_query,
}))
sys.exit(0)
PY
)"
RC=$?

if [[ ${RC} -ne 0 || -z "${RESULT}" ]]; then
  eval_err "AC-2 measurement failed (DuckDB error or warehouse lock)."
  exit "${EVAL_ERROR}"
fi

# Pretty-print the per-query and aggregate numbers, then decide the gate.
SUMMARY="$(
  AC2_RESULT="${RESULT}" "${PYEXEC[@]}" - <<'PY'
import json, os
r = json.loads(os.environ["AC2_RESULT"])
for name, m in r["per_query"].items():
    print(f"QQ {name:<22} p50={m['p50_ms']:>8.2f}ms  p95={m['p95_ms']:>8.2f}ms  max={m['max_ms']:>8.2f}ms")
print(f"AGG p50={r['agg_p50_ms']:.2f}ms  p95={r['agg_p95_ms']:.2f}ms  max={r['agg_max_ms']:.2f}ms  "
      f"(over {r['total_samples']} samples, {r['n_queries']} queries x {r['reps_per_query']} reps)")
print(f"PASSED {r['passed']}")
print(f"AGGP95 {r['agg_p95_ms']}")
print(f"BUDGET {r['budget_ms']:.0f}")
PY
)"

while IFS= read -r line; do
  case "${line}" in
    QQ\ *) eval_info "${line#QQ }" ;;
  esac
done <<<"${SUMMARY}"

AGG_LINE="$(grep '^AGG ' <<<"${SUMMARY}")"
eval_info "${AGG_LINE#AGG }"

PASSED="$(sed -n 's/^PASSED //p' <<<"${SUMMARY}")"
AGGP95="$(sed -n 's/^AGGP95 //p' <<<"${SUMMARY}")"

if [[ "${PASSED}" == "True" ]]; then
  eval_pass "AC-2: gold query aggregate p95 ${AGGP95} ms <= ${BUDGET_MS} ms budget."
  exit "${EVAL_PASS}"
fi

eval_fail "AC-2: gold query aggregate p95 ${AGGP95} ms exceeds the ${BUDGET_MS} ms budget."
exit "${EVAL_FAIL}"
