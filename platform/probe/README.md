# platform/probe — C8 active freshness probe (AC-3 gate)

Proves **AC-3: freshness** — that an order committed in the source Postgres is
queryable in the gold layer within **5 minutes (300 s)**. Unlike a passive
watermark reader, this probe MEASURES the real end-to-end pipeline by driving it
once per sample, then gates on the **median** sample lag.

This is a *measured* gate, not an asserted one (R5): it injects a real beacon
order, runs the real C2 ingest + dbt build, times source-COMMIT → gold-visible,
and emits a pass/fail verdict whose exit code is the contract.

> Contributor handbook for `probe/`. Package-wide context (the asset graph, the
> single-writer rule, the env contract) lives in [`../README.md`](../README.md);
> the warehouse it drives is owned by [`../warehouse/README.md`](../warehouse/README.md).
> Both are referenced here, not duplicated.

---

## How it works (one sample)

```
  1. INJECT  a clean "beacon" order into SOURCE Postgres        → record t0 (after COMMIT)
  2. RUN     C2 ingestion       (Postgres → raw.raw_orders)
  3. RUN     dbt build          (raw → bronze → silver → gold)
  4. POLL    gold.gold_orders_obt (read-only) for the beacon    → record t1 when visible
  5. lag = t1 - t0
```

Repeating gives a distribution; **AC-3 = median(lag) ≤ 300 s**. The single-shot CI
mode (`--ci`) runs exactly one sample.

- **The beacon is an ordinary clean order** — valid status (`placed`), positive
  money/quantity, a fresh `ordered_at` — so the medallion classifier accepts it
  and never dedups or quarantines it. FK references are resolved live from the
  source.
- **`order_id` is the beacon key.** `gold_orders_obt` is at order grain and exposes
  `order_id` directly, so the probe matches its own injected row exactly with no
  model change.
- **An ephemeral C2 ingest** is used deliberately: a single-shot probe must not
  depend on `DAGSTER_HOME` watermark state, and the raw upsert is idempotent. The
  C2 PK arm captures the brand-new `order_id` regardless of watermark position.

### The AC-3 verdict

```
PASS  iff  median(end_to_end_lag_s over samples)  <=  AC3_BUDGET_S   (300 s)
```

The **median** is the gate (the freshness SLO the brief commits to), not the max —
but min/max and a per-stage breakdown (ingest / dbt / gold-poll) are reported so a
tail breach is visible. If a beacon never reaches gold within the per-sample
timeout, that is a real AC-3 failure (the pipeline lost the row), not a skip.

---

## Running

```bash
# CI / fast gate: single-shot, JSON
uv run python -m platform.probe.cli --ci
#   or:
make eval-ac3            # drives this probe via platform/evals/eval_ac3.sh

# Several samples, human-readable report (median is the statistic)
uv run python -m platform.probe.cli --samples 3

# Override the budget / per-sample timeout; keep beacons for inspection
uv run python -m platform.probe.cli --budget-s 300 --sample-timeout-s 600 --no-cleanup
```

> **Single-writer.** The probe runs C2 ingest then dbt build — the warehouse's two
> writers — strictly in that order. Each writer step retries on a transient DuckDB
> lock conflict (waiting is the correct response under the C4 contract); a stuck
> lock surfaces as an error, not a hang.

### Exit codes (the gate)

| code | meaning |
|------|---------|
| 0 | AC-3 **PASS** — median lag ≤ budget |
| 1 | AC-3 **FAIL** — median lag > budget, or a beacon never reached gold |
| 2 | precondition / pipeline error — the gate could not run |
| 77 | **SKIPPED** — source Postgres unreachable; AC-3 is unmeasurable (never a silent pass) |

A DB-down condition is a SKIP, not a failure: AC-3 cannot be measured without the
live pipeline, and silently passing or failing would be dishonest.

## Preconditions

- Postgres up (`make up`) and seeded (`make seed`) — the beacon needs FK-valid
  `customers`/`products`.
- The pipeline must be runnable (C2 + dbt resolve through the active venv); the
  probe runs them itself per sample.

---

## Cleanup is part of the gate (R7)

The probe is reversible by construction. On teardown it:

1. **Removes its beacons from the SOURCE** (`remove_beacons`), and
2. **Purges the same `order_id`s from `raw` + `gold`** in the warehouse
   (`purge_beacons_from_warehouse`).

Step 2 is not optional: **C2 is upsert-only and never deletes**, so a source-only
cleanup would leave every ingested beacon in `raw.raw_orders` and propagate it to
`gold.gold_orders_obt` forever — each run would permanently inflate gold with
synthetic orders and skew AC-2/AC-3 and any downstream eval. Cleanup is
best-effort: a teardown failure never masks the measurement result.

## Boundaries (R3 one-way dependency)

The probe is a **read-only** consumer of the warehouse for verification and a
**writer only of its own beacon rows** (into the source, then a brief warehouse
delete to purge them). It owns and removes exactly its own rows; it never repairs
real data and never imports or triggers Component B.

---

## Files

| file | role |
|------|------|
| [`freshness.py`](freshness.py) | `inject_beacon` / `run_ingest` / `run_dbt_build` / `wait_for_gold` / `measure_once` / `run_probe`; the R7 source + warehouse purge; single-writer lock-retry. `AC3_BUDGET_S`, `SampleResult`, `ProbeRun`, `ProbeError`. |
| [`verdict.py`](verdict.py) | `build_report` (median-lag gate) → `ProbeReport`; `render_text`. |
| [`cli.py`](cli.py) | `--ci` / `--samples` entrypoint; the gate exit codes (0/1/2/77). |
