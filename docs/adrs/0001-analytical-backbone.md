# ADR-0001: Analytical backbone — locked decisions for Component A

**Status:** Accepted
**Date:** 2026-06-29

> Scope: this ADR records the decisions that gate Component A (the deterministic
> analytical backbone, `platform/`). The two-component split, the open-decision
> framing (U1–U3), and the operating rules are defined in
> [`../../CLAUDE.md`](../../CLAUDE.md) and
> [`../../.claude/rules/agent-operating-rules.md`](../../.claude/rules/agent-operating-rules.md)
> and are referenced, not restated, below.

## Context

Component A lifts an e-commerce source out of Postgres, lands it in a single
DuckDB warehouse, refines it through a dbt medallion, and exposes a clean gold
layer. Three of its boundaries were explicitly **open** in the brief (U1–U3), and
two implementation choices proved load-bearing enough to warrant a record because
they will be re-litigated by anyone touching ingestion or the warehouse. This ADR
freezes the five decisions that the now-built, green-end-to-end backbone depends
on. The corresponding `<!-- TODO -->` "open decision" framing in the rules is
considered resolved *for Component A as scoped* by what is recorded here.

---

## Decision 1 — U2: Dagster owns the raw landing

The raw landing zone (`raw.raw_*` in DuckDB) is owned by **Dagster ingestion
(C2)**, not by a dbt-bronze stage. dbt's `source('raw', 'raw_*')` reads tables
that C2 has already written; bronze is the first *transformation*, not the
extractor.

- C2 mirrors the source 1:1 and is **defect-faithful**: negative prices, NULLs,
  drifted columns, and orphan payments all land verbatim. The medallion — not the
  extractor — decides what is a defect.
- The join seam is the asset key. dbt's default translator maps source
  `raw.raw_orders` to `AssetKey(["raw", "raw_orders"])`, exactly the key the C2
  `@asset` publishes, so bronze attaches downstream automatically and one
  "Materialize all" runs ingest → dbt. The contract is asserted at definition-load
  time in `platform/ingestion/dbt_assets.py` (`_MedallionDbtTranslator`), so a dbt
  source rename fails loud instead of silently orphaning bronze.

**Why:** a single owner of the raw boundary keeps extraction and refinement
responsibilities clean and makes the raw schema a stable contract dbt builds on.

---

## Decision 2 — U3: quarantine, don't drop (detection seam)

Defects are injected into raw Postgres; they must remain **detectable** through
the medallion. Silver **quarantines** every rejected row into
`silver_<entity>_rejects` (tagging `reject_rule` == the generator's
`failure_key`) and **never drops** it. Gold is built over accepted rows only, so
it is clean *by construction* without losing the audit trail.

- The defect is visible in **silver** (`silver_*_rejects`), absent from **gold**.
  That is the named answer to U3's "which layer is the defect visible in?".
- On the incremental (non-full-refresh) path, the `_ingested_at` processing-time
  watermark plus the `evict_reprocessed_window` pre-hook guarantee a row that
  transitions accepted→rejected (e.g. `destructive_fix`, `malformed_data`) is
  evicted from `silver_orders` and re-lands in `silver_orders_rejects` — a stale
  clean row never lingers in gold. See `models/silver/silver_orders.sql`.
- The `eval_defect_survival.sh` eval asserts BOTH halves: caught in the right
  rejects table AND count = 0 in `gold_orders_obt`.

**Why:** a dropped defect is invisible; a quarantined defect is evidence. This is
also what makes the future Sentinel's detection scoring possible against the
`injected_incidents` oracle without coupling A to B.

---

## Decision 3 — CDC via timestamp-incremental extraction

C2 extracts incrementally using a **timestamp high-watermark** read from the last
successful materialization's metadata (cold start = full load). There is no CDC
log on the source, so the event-time watermark is supplemented with two bounded
PK arms for orders/payments:

- **PK-new arm** — captures a brand-new PK above the prior high PK regardless of
  (possibly backdated) event time → catches `late_arrival` without a 45-day
  re-scan.
- **PK-recency arm** — re-extracts rows within `PK_REFRESH_WINDOW` of the prior
  high PK regardless of event time → catches in-place UPDATEs to recent rows
  (`destructive_fix`, `malformed_data`) that do not move `ordered_at`, re-stamping
  a fresh `_ingested_at` so silver re-classifies them.

The named precondition (an in-place UPDATE to a row older than both the lookback
and the PK band is not detectable by timestamp + bounded-PK CDC alone) is
documented in `platform/ingestion/assets.py`; the generator injects no such case,
and the default ingest path is a full reload, so it is sound as scoped.

**Why:** it keeps AC-2/AC-3 safe (no full re-scans) while still catching every
chaos mode the generator injects, which always target recent rows.

---

## Decision 4 — the bulk-upsert fix (set-based Arrow write)

The raw upsert is a single **set-based, vectorized** `INSERT … SELECT … ON
CONFLICT` over an in-memory Arrow table, not a per-row `executemany`.

- The earlier `executemany` re-planned and re-executed the upsert once per row —
  800k+ prepared-statement executions, each doing its own index conflict-check —
  making the orders full load take 16+ minutes at pegged CPU.
- The fix assembles the whole batch into one Arrow table, registers it as a
  virtual relation, and writes it with one statement (the DuckDB Python docs
  explicitly warn against `executemany` for bulk loads). Typed target DDL casts on
  INSERT, so Arrow type inference never compromises defect fidelity. See
  `platform.ingestion.assets._upsert`.

**Why:** the per-row path made full loads operationally unusable; the set-based
write is the supported bulk-ingest idiom and restores end-to-end runtime.

---

## Decision 5 — wire dbt into the Dagster asset graph (one DAG)

The dbt medallion is exposed as Dagster assets via `@dbt_assets`, so C2 ingestion
and C3 transformation form **one connected DAG** rather than two disjoint
pipelines.

- `platform/ingestion/dbt_assets.py` turns every dbt node into a Dagster asset and
  asserts the raw-source → ingestion-key contract (Decision 1).
- A single named job, `backbone_end_to_end` (`AssetSelection.all()`,
  `max_concurrent=1`), runs the whole 18-asset graph serially and leaves one
  SUCCESS row on the timeline.
- Single-writer is preserved three ways: topology serializes the raw→dbt seam,
  `dbt build` is one Dagster step, and the dbt profile pins `threads: 1`.

**Why:** one graph gives one lineage view, one runnable job, and a single place to
enforce the DuckDB single-writer contract — instead of an out-of-band dbt run the
orchestrator cannot see.

---

## Consequences

**Positive:**
- One "Materialize all" / `backbone_end_to_end` run executes ingest → dbt end to
  end, in correct order, with no concurrent warehouse writers.
- Defects are auditable (quarantined, not dropped); gold is clean by construction.
- Full loads run in reasonable time; chaos modes targeting recent rows are caught
  without expensive re-scans.
- The detection seam is named and testable, which keeps Component A independent of
  Component B (R3) while still enabling future scoring.

**Negative / trade-offs:**
- The single-writer rule is a real operational constraint: evals, the probe, and
  warehouse-touching tests must serialize, and a stale file holder must be cleared
  before retrying.
- The CDC scheme carries a named precondition (Decision 3): an in-place UPDATE to
  a row older than both the lookback and the PK band is not detectable by the
  incremental path alone. Removing it needs a processing-time source column or a
  dimension PK-recency arm — deferred as out of scope.
- The `platform` package name shadows the stdlib `platform` module, so every entry
  point must set `PYTHONPATH=<repo-root>`.

**Neutral:**
- `ingestion/assets.py` and `ingestion/dbt_assets.py` deliberately omit
  `from __future__ import annotations` (Dagster runtime-introspects the `context`
  type hint). Intentional, not an oversight.
- The `backbone_every_15min` schedule ships STOPPED (operator-armed, mutates the
  warehouse); the `backbone_failure_logger` sensor ships RUNNING (observes only).

## Citations

- Project handbook: [`../../CLAUDE.md`](../../CLAUDE.md) (component split, AC table, U1–U3 framing)
- Operating rules: [`../../.claude/rules/agent-operating-rules.md`](../../.claude/rules/agent-operating-rules.md) (R3 one-way dependency, R5 verify-by-kind, R7 reversibility)
- Code — Decision 1/5: `platform/ingestion/dbt_assets.py`, `platform/ingestion/definitions.py`, `platform/ingestion/jobs.py`
- Code — Decision 2: `platform/transform/models/silver/silver_orders.sql`, `platform/transform/models/silver/silver_orders_rejects.sql`, `platform/evals/eval_defect_survival.sh`
- Code — Decision 3: `platform/ingestion/assets.py` (`LOOKBACK`, `PK_REFRESH_WINDOW`, the three extraction arms)
- Code — Decision 4: `platform/ingestion/assets.py` (`_upsert`)
