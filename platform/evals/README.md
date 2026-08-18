# platform/evals — acceptance-criteria evals

Executable, self-scoring bash evals for Component A's acceptance criteria. Each
eval MEASURES a real number against the live stack and prints an explicit
`PASS` / `FAIL` / `SKIP` verdict; the **exit code is the contract**.

| Eval | Criterion | What it measures | Drives |
|------|-----------|------------------|--------|
| `eval_ac1.sh` | **AC-1** peak isolation | analytics-attributable lock-waits on the OLTP path under peak load (target: 0) | `platform.harness` (C4h) |
| `eval_ac2.sh` | **AC-2** query latency | aggregate p95 over a basket of gold OBT queries (budget: ≤ 5000 ms) | DuckDB gold, read-only |
| `eval_ac3.sh` | **AC-3** freshness | median source→gold lag for a beacon order (budget: ≤ 300 s / 5 min) | `platform.probe` (C8) |
| `eval_defect_survival.sh` | **U3** detection seam | each injected defect is caught in `silver_<entity>_rejects` and absent from gold | `src.gen` → C2 → dbt |
| `run_evals.sh` | all of the above | runs every eval, prints a verdict table | the four evals |

## Exit-code contract (every `eval_*.sh`)

| Code | Meaning |
|------|---------|
| `0`  | **PASS** — criterion measured and met |
| `1`  | **FAIL** — criterion measured and breached |
| `2`  | **ERROR** — precondition/pipeline error; gate could not run |
| `77` | **SKIP** — required infra missing; gate is unmeasurable (never silently passes) |

`run_evals.sh` returns `1` if any eval failed/errored, `0` otherwise (skips are
not counted as failures, but an all-skip run prints `INCONCLUSIVE`).

## Prerequisites

```bash
make up            # Postgres healthy on :5432
make seed          # clean baseline (500 / 200 / 5000 / 5000)
make ingest-once   # C2: Postgres -> DuckDB raw
make dbt-build     # C3: raw -> bronze/silver/gold (materializes gold)
```

`eval_ac1` / `eval_ac3` / `eval_defect_survival` need Postgres up; `eval_ac2` /
`eval_ac3` / `eval_defect_survival` need a materialized `gold` schema. Any missing
prerequisite yields a `SKIP` (77), not a false pass.

## Running

```bash
make evals                       # all evals, scaled-down (smoke AC-1, 1-sample AC-3)
make eval-ac1 PROFILE=full       # the real 75k/day-equiv AC-1 gate
make eval-ac2 REPS=50            # AC-2 with more reps
make eval-ac3 SAMPLES=3          # AC-3 median of 3 samples
make eval-defects                # defect-survival sweep

# or directly:
bash platform/evals/run_evals.sh --ac1-profile full --ac3-samples 3
bash platform/evals/run_evals.sh --only ac2,ac3
bash platform/evals/eval_defect_survival.sh negative_price orphan_payment
```

## Design notes

- **Scaled-down by default.** `eval_ac1` uses the harness `smoke` profile (same
  attribution code path, short window) so CI runs in seconds; pass `full` for the
  contract-scale gate. `eval_ac2` defaults to 30 reps/query; `eval_ac3` to a
  single CI sample.
- **Defect→reject mapping is derived from `classify.sql`.** `reject_rule` IS the
  generator `failure_key`, so the eval joins `silver_<entity>_rejects.reject_rule`
  to the injected failure. `orphan_payment` lands in `silver_payments_rejects`;
  the order-level defects in `silver_orders_rejects`.
- **U3 contract (locked):** silver QUARANTINES rather than drops. The eval asserts
  BOTH that the defect was caught (`count ≥ 1` in the right rejects table) AND that
  it did NOT leak to gold (`count = 0` in `gold_orders_obt`).
- **R7 reproducibility:** `eval_defect_survival` restores a known-clean baseline by
  **truncate + deterministic reseed** (`src.seed.seed --truncate --seed 42`) before
  every injection. A surgical row-delete cannot be used because UPDATE-style
  injectors (`destructive_fix`, `malformed_data`) corrupt existing baseline rows in
  place; only a reseed is safe.
- **C4 single-writer contract:** the warehouse is single-writer-process. The
  defect eval polls `wait_for_writable` before each writer step so it never starts
  an ingest/dbt into a held lock (an external tool — e.g. an IDE DuckDB extension —
  may briefly hold the file). This is the contract enforced as a gate, not a
  workaround.
