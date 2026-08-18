# platform/ — Component A: the analytical backbone

The deterministic half of the system. A high-volume e-commerce company runs its
storefront and its analytics on one Postgres database; the two workloads
compete. This package is the purpose-built backbone that separates them: it lifts
the source out of Postgres, lands it in a DuckDB warehouse, refines it through a
dbt medallion, and exposes the clean gold layer as typed analytical tools — then
proves the result against six acceptance criteria.

> This README is the contributor handbook for `platform/`. Project-wide context
> (the two-component split, agent fleet, operating rules, open decisions) lives in
> [`../CLAUDE.md`](../CLAUDE.md) and [`../.claude/rules/agent-operating-rules.md`](../.claude/rules/agent-operating-rules.md);
> it is referenced here, not duplicated. The locked architectural decisions are
> recorded in [`../docs/adrs/0001-analytical-backbone.md`](../docs/adrs/0001-analytical-backbone.md).

---

## WHAT — the six components

`platform/` is one connected pipeline plus its verification surfaces. The source
Postgres + chaos generator live in [`../src`](../src); everything below reads from
there.

| Pkg | Code | Role | Reads | Writes |
|-----|------|------|-------|--------|
| [`ingestion/`](ingestion) | **C2** | Dagster software-defined assets: Postgres → DuckDB `raw.raw_*`, timestamp-incremental (CDC). Also wires the dbt medallion into the SAME asset graph. | Postgres (read-only) | DuckDB `raw` |
| [`transform/`](transform) | **C3** | dbt medallion: `raw` → bronze → silver (+ `*_rejects`) → gold. 14 models. Quarantines defects, never drops them. | DuckDB `raw`/`bronze`/`silver` | DuckDB `bronze`/`silver`/`gold` |
| [`warehouse/`](warehouse) | **C4** | The DuckDB substrate: the one place a connection is opened, the one place the file path resolves. Single-writer-process. | — | — (owns the file) |
| [`intelligence/`](intelligence) | **C5** | FastAPI REST facade + mounted MCP server over the read-only gold layer. Three tools: `get_schema_info`, `execute_analytical_query`, `generate_report`. | DuckDB `gold` (read-only) | — |
| [`harness/`](harness) | **C4h** | Peak-load harness — the AC-1 gate. N writers + M analytics readers + a lock-wait monitor against the live source. | Postgres | Postgres (its own load) |
| [`probe/`](probe) | **C8** | Active freshness probe — the AC-3 gate. Injects a beacon order, runs the pipeline, times source→gold lag. | Postgres + DuckDB | a self-purged beacon row |

Supporting: [`evals/`](evals) — executable, self-scoring acceptance-criteria
evals (bash; exit code is the contract). See [`evals/README.md`](evals/README.md).

### The asset graph (one connected DAG)

```
Postgres (src/)                    DuckDB warehouse (warehouse/)
  customers ─┐
  products  ─┤   C2 ingestion          C3 dbt medallion
  orders    ─┼──▶ raw.raw_* ──────────▶ bronze ─▶ silver ─▶ gold ──▶ C5 tools
  payments  ─┘   (4 @asset)            └─▶ silver_*_rejects (quarantine)
                                                              │
                              C4h ──(AC-1 peak isolation)     ├──▶ C8 (AC-3 freshness)
                                                              └──▶ AC-2 (gold p95)
```

C2's four raw assets and the dbt models share **one** DuckDB file. dbt's
`source('raw', 'raw_*')` resolves to the *same* Dagster asset keys C2 publishes
(`raw/raw_*`), so bronze sits downstream of ingestion and one "Materialize all"
runs the whole thing end to end. The join seam is asserted at load time — see
[`ingestion/dbt_assets.py`](ingestion/dbt_assets.py).

---

## WHY — the design in one paragraph each

- **Why separate raw landing from transformation (C2 vs C3).** Ingestion must be
  *defect-faithful*: it mirrors the source 1:1 (negative prices, NULLs, drifted
  columns, orphan payments all ride through verbatim) so the medallion — not the
  extractor — decides what is a defect. C2 owns raw; C3 owns refinement. This is
  the U2 decision (Dagster-owns-raw), now locked.
- **Why quarantine, not drop (C3).** A dropped defect is invisible; a quarantined
  one is evidence. Silver routes every rejected row into `silver_<entity>_rejects`
  with the `reject_rule` (== the generator's `failure_key`) and keeps it forever.
  Gold is clean *by construction* (built over accepted rows only) without losing
  the audit trail. This is the U3 decision (quarantine-not-drop), now locked.
- **Why a single DuckDB file with a single-writer rule (C4).** DuckDB is
  single-writer-process. Every writer (C2, C3) routes through one connection
  helper and runs serially (topology + `max_concurrent=1` + dbt `threads: 1`);
  every reader (C5, C4h, C8) opens read-only and may run concurrently. One file,
  one rule, no "database is locked" by construction.
- **Why curated intents, not raw SQL (C5).** The LLM never hands us SQL. It picks
  a named intent; the engine binds parameters into a static template that touches
  only allowlisted gold tables, opens the warehouse read-only, and passes every
  statement through a write-blocking guard. Two independent layers each fully
  block a write; the gold allowlist is upheld by the templates.
- **Why active gates, not assertions of belief (C4h / C8 / evals).** AC-1 and
  AC-3 are *measured* against the live stack, not claimed. C4h drives real peak
  load and audits lock-waits attributable to the analytics session; C8 drives a
  real beacon order through the whole pipeline and times it. The exit code is the
  verdict.

---

## HOW — running the backbone

All commands are from the repo root. The environment contract (set these or use
the Makefile, which sets them for you):

```bash
export PYTHONPATH=$(pwd)          # the local `platform/` package shadows stdlib platform
export DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb
export DAGSTER_HOME=$(pwd)/.dagster_home
```

> **DuckDB is single-writer.** Never run two warehouse-writing steps
> concurrently (C2 ingest, C3 dbt build, the C8 probe, the defect eval all write).
> If a step reports "database is locked", a stale holder is open — clear it with
> `lsof -t platform/warehouse/warehouse.duckdb | xargs -r kill -9` before retrying.

### 1. Bring up the source and seed it

```bash
make up            # PostgreSQL healthy on :5432 (schema auto-applied)
make seed          # clean correlated baseline (500 customers / 200 products / 5000 orders)
```

### 2. Run the full pipeline end to end

The named Dagster job `backbone_end_to_end` runs the whole 18-asset graph
(C2 raw ingestion → C3 dbt medallion), serialized to honor single-writer.

Two ways to run it:

```bash
# A) Headless (CI, scripts, the probe): in-process materialize, no webserver.
make ingest-once    # C2 ingest, incremental via DAGSTER_HOME
make dbt-build      # C3 medallion: raw -> bronze/silver/gold
# or the whole graph in one process:
uv run python -m platform.ingestion.run --persistent

# B) Interactive (UI on :3000): launch Dagster dev, then run the job.
make dagster-dev    # http://localhost:3000
#   In the UI: Jobs -> backbone_end_to_end -> Materialize all.
#   (Do NOT launch a second `dagster dev` — one daemon at a time.)
```

The graph is loaded from [`ingestion/definitions.py`](ingestion/definitions.py),
which also registers:
- **`backbone_end_to_end`** (job) — the one-click end-to-end pipeline.
- **`backbone_every_15min`** (schedule) — STOPPED by default; operator-armed,
  because it mutates the warehouse and would race the evals.
- **`backbone_failure_logger`** (sensor) — RUNNING by default; logs only. Emits a
  stable `BACKBONE_RUN_FAILURE` line on any failed run (the future Sentinel's I1
  evidence surface). It never imports or triggers Component B (R3).

### 3. Serve the gold layer (C5)

```bash
# MCP over stdio (Claude Desktop and other LLM hosts):
uv run python -m platform.intelligence.server
# REST + MCP over HTTP (dashboards, the eval harness, smoke tests):
uv run python -m platform.intelligence.server --http     # :8000, MCP at /mcp
uv run uvicorn platform.intelligence.app:app             # FastAPI app: /api, /healthz, /mcp
```

---

## WHERE — running the evals and the AC verdicts

The six acceptance criteria are the brief's pass/fail gates (full table in
[`../CLAUDE.md`](../CLAUDE.md)). The four that `platform/` measures directly are
covered by [`evals/`](evals); each prints `PASS` / `FAIL` / `SKIP` and the exit
code is the contract (`0` PASS · `1` FAIL · `2` ERROR · `77` SKIP).

```bash
make evals                  # ALL evals, scaled-down (smoke AC-1, 1-sample AC-3) + verdict table
make eval-ac1 PROFILE=full  # the real 75k/day-equiv AC-1 gate
make eval-ac2 REPS=50       # AC-2 gold p95 with more reps
make eval-ac3 SAMPLES=3     # AC-3 median of 3 samples
make eval-defects           # U3 defect-survival sweep

# direct, with options:
bash platform/evals/run_evals.sh --ac1-profile full --ac3-samples 3
bash platform/evals/run_evals.sh --only ac2,ac3
```

### Acceptance-criteria verdicts

| Eval | AC | Budget | What it measures | Verdict |
|------|----|--------|------------------|---------|
| `eval_ac1.sh` | **AC-1** peak isolation | 0 analytics-attributable lock-waits on the OLTP path under peak load | C4h drives writers + analytics readers, audits lock-waits by `application_name='dagster_ingest'` | _record from the captured run_ |
| `eval_ac2.sh` | **AC-2** query latency | p95 ≤ 5000 ms over a basket of gold OBT queries | times read-only gold queries, aggregates p95 | _record from the captured run_ |
| `eval_ac3.sh` | **AC-3** freshness | median source→gold lag ≤ 300 s (5 min) | C8 injects a beacon order, runs ingest + dbt, times until it is queryable in gold | _record from the captured run_ |
| `eval_defect_survival.sh` | **U3** detection seam | every injected defect caught in `silver_*_rejects`, absent from gold | inject → C2 → dbt → assert quarantined and not leaked | _record from the captured run_ |

> The verdict cells are intentionally unfilled in this committed copy: the evals
> exist and are runnable, but a verdict must be the **measured** result of an
> actual run, never an asserted one (the project honesty rule, and R5 — Component A
> is verified by assertion against real numbers). After `make evals` (or the
> per-AC targets) completes, paste the printed verdict + measured value into each
> cell. AC-4/AC-5/AC-6 are operational/consumer outcomes, not measured by
> `platform/` directly.

Prerequisites and the full eval design (scaling, defect→reject mapping, R7
reseed-to-clean, the single-writer `wait_for_writable` gate) are documented in
[`evals/README.md`](evals/README.md).

---

## Conventions specific to this package

These extend (don't replace) the repo-wide rules in
[`../.claude/rules/agent-operating-rules.md`](../.claude/rules/agent-operating-rules.md):

- **The top-level package is named `platform`**, which shadows the stdlib
  `platform` module. Always run with `PYTHONPATH=<repo-root>` so the local package
  resolves first. `python -m` does this implicitly; the bare `dagster` CLI does
  not (the Makefile exports it).
- **`ingestion/assets.py` and `ingestion/dbt_assets.py` deliberately omit
  `from __future__ import annotations`** — Dagster introspects the `context`
  parameter's runtime type hint at decoration time, and stringized annotations
  break that check. This is intentional; don't "fix" it.
- **One connection helper, one path resolver.** Open the warehouse only through
  [`warehouse/connection.py`](warehouse/connection.py); resolve its path only
  through [`warehouse/paths.py`](warehouse/paths.py) (canonical env var:
  `DUCKDB_DATABASE`). No other file hardcodes a warehouse path.
- **Defects are data, not errors.** C2 never repairs or drops; C3 quarantines into
  `*_rejects`. If you find yourself filtering a defect out in ingestion, it
  belongs in silver instead.

## Tests

```bash
uv run ruff check src platform tests        # lint (make lint)
PYTHONPATH=$(pwd) DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb \
  DAGSTER_HOME=$(pwd)/.dagster_home uv run pytest
```

Coverage spans C2 ingestion, the C4h harness, C5 intelligence, the C8 probe, and
two end-to-end backbone tests (full + incremental medallion). The same
single-writer rule applies — pytest serializes warehouse-touching tests.
