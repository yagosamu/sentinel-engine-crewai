# Sketch · Analytical Backbone

The **deterministic platform** — Layers 1–5. Extracts from the transactional
PostgreSQL source, transforms via dbt Medallion, materializes in DuckDB, and
exposes analytics through an MCP server. Same input → same output; verified by
tests and data assertions (not by judgement — that's the Sentinel's job, see
`sentinel-engine.md`).

This is **Component A** of the two-component split. It is self-sufficient: it
delivers the brief's value with no agents present. The Sentinel (Component B)
reads A's exhaust but A never depends on B.

> Plan altitude: features, components, dependencies, build order, acceptance-
> criteria mapping. No atomic tasks, no code.

---

## Components

### C1 · Source (Layer 1) — *built*

PostgreSQL transactional DB: `customers`, `products`, `orders`, `payments`, plus
the `injected_incidents` ledger. Populated by the seeder (clean baseline) and the
chaos generator (traffic + 14 injected failures).

- **Does:** the realistic source the whole platform reads from; originates the
  failures the Sentinel must catch; records ground truth to `injected_incidents`.
- **Depends on:** nothing.
- **Status:** built (`src/db`, `src/seed`, `src/gen`).

### C2 · Ingestion (Layer 2) — Dagster
Software-defined assets that incrementally extract Postgres → raw warehouse
tables. Manages lineage and run dependencies.

- **Does:** incremental/CDC-style extraction; one asset per source table; emits
  run metadata + logs (the Sentinel's Log Analyst reads these).
- **Depends on:** C1 (source schema), C4 (warehouse target).
- **Serves:** **AC-3** (freshness, via incremental sync); the sync-without-errors
  outcome. Produces the log surface the Sentinel observes.

### C3 · Transformation (Layer 3) — dbt Medallion
`bronze_` (raw mirrors) → `silver_` (cleansed, deduped, typed) → `gold_`
(business aggregates / One-Big-Tables).

- **Does:** bronze/silver/gold models; dbt tests as data assertions; gold OBTs
  optimized for query.
- **Depends on:** C2 (raw tables in warehouse).
- **Serves:** clean, queryable analytics; schema-change resilience; emits dbt run
  results (Log Analyst reads these too).

### C4 · Warehouse (Layer 4) — DuckDB
Embedded analytical engine the models materialize into and the MCP server
queries. Cloud path: swap file for MotherDuck, no SQL rewrites.

- **Does:** the analytical substrate — the "separate lane" that gets analytics
  off Postgres so the two jobs stop competing.
- **Depends on:** nothing (substrate C2/C3 write to, C5/C4h/C8 read from). Stood
  up alongside C2.
- **Serves:** **AC-2** (p95 ≤ 5s / p99 ≤ 15s query latency); the Data Profiler
  queries gold/silver here.

### C5 · Intelligence (Layer 5) — FastAPI + MCP

MCP server exposing three tools over `gold_`: `get_schema_info`,
`execute_analytical_query`, `generate_report`. Natural-language access via Claude
Desktop.

- **Does:** self-serve analytics for non-technical users.
- **Depends on:** C4 (warehouse), C3 (gold tables to expose).
- **Serves:** data democratization (5–10 → 30–40 business users querying).
- **⚠ Scope flag:** the brief scopes the natural-language layer **out** ("a
  consumption-experience problem — evaluate after the foundation is proven").
  In this program only if Phase 0 keeps it. See **U1**.

---

## Components the spec omits but the brief requires

These satisfy acceptance criteria nothing else covers. Not optional if the brief
is the source of truth.

### C4h · Peak-load harness — *missing*
Drives **75,000 orders/day** against C1 with analytics running hot, and measures
transactional commit latency + analytics-attributable lock-waits.

- **Does:** proves isolation under peak. The current generator is a chaos *drip*
  (fixed batch + sleep), **not** a peak-load tool.
- **Depends on:** C1.
- **Serves:** **AC-1** — the brief's designated early go/no-go gate. Build first.

### C8 · Freshness probe — *missing*
Continuously samples source-commit time → gold-availability time.

- **Does:** measures end-to-end lag; the only thing that proves freshness.
- **Depends on:** C2, C3.
- **Serves:** **AC-3** (≤ 5 min median freshness).

> **Pricing cadence (AC-4)** and **maintenance/incident KPIs (AC-5, AC-6)** are
> operational outcomes measured *on* this platform, not components built *in* it.
> Tracked in the brief; noted here so the AC map is complete.

---

## Acceptance-criteria map

| AC | What it proves | Covered by |
| --- | --- | --- |
| AC-1 | Peak isolation (go/no-go gate) | **C4h** |
| AC-2 | Query latency p95 ≤ 5s | C4, C3 (gold OBTs) |
| AC-3 | Freshness ≤ 5 min | C2 (incremental), **C8** (measures it) |
| AC-4 | Intraday pricing cadence | platform consumer — outcome, not a component |
| AC-5 / AC-6 | Incident load / maintenance down | operational outcome of the whole split |

---

## Dependencies & build order

```text
C1 Source ──► C2 Ingestion ──► C3 Transform ──► C5 Intelligence
 (built)       Dagster   │       dbt        │     FastAPI/MCP
                         ▼                   ▼
                   (C4 Warehouse — substrate; stood up alongside C2)

C4h Peak-load ◄─ C1     (parallel track, gates the program)
C8 Freshness  ◄─ C2,C3  (lands after C3)
```

**Phase 0 — scope decision (blocks all):** brief = separation only; spec = full
agentic stack. Resolve **U1** before any task is written — it decides whether C5
is in this program.

**Phase 1 — foundation & gates:**
1. **C4h peak-load harness + AC-1 measurement** — go/no-go gate; nothing exists.
2. C2 Dagster ingestion → C4 DuckDB landing (built together).
3. C3 dbt bronze → silver → gold.
4. **C8 freshness probe (AC-3)** — once C3 lands.

**Phase 2 — intelligence (only if Phase 0 keeps it):**
5. C5 FastAPI + MCP on gold tables.

Each stage needs the previous stage's output to exist before it's testable.

---

## Interface exposed to the Sentinel (Component B)

Read-only contract — A produces, B observes. See `sentinel-engine.md`.

- **Dagster run logs / asset status** (C2) — Log Analyst.
- **dbt run results** (C3) — Log Analyst.
- **DuckDB `gold_`/`silver_` tables** (C4) — Data Profiler.
- **`injected_incidents` + failure signature** (C1) — ground-truth scoring oracle.

---

## Open / unresolved

- **U1 · Scope conflict (Phase 0).** Brief scopes C5 *out*; spec makes it central.
  Unresolved = inverted priorities. *(Owner: CTO / VP Data.)*
- **U2 · C2/C3 raw boundary.** Spec says Dagster lands "directly into raw DuckDB"
  *and* that dbt bronze is the "raw mirror." Decide which owns raw — sets the
  C2↔C3 interface.
- **U3 · Detection seam (→ Component B).** Failures are injected into raw Postgres
  (often by dropping constraints); the Profiler is specced against gold DuckDB. If
  silver cleans them, defects vanish before B looks. The contract above must
  define which defects survive to which layer. Blocks B's planning.
- **Freshness vs. "real-time":** a Dagster→dbt batch pipeline is *minutes-fresh*,
  not true real-time. AC-3 asks ≤ 5 min, which batch can meet — confirm minutes
  suffices, don't over-build true real-time. *(Owner: VP Data.)*
- **CDC mechanism for C2** (true CDC vs. incremental-by-timestamp) — decide at
  task time; affects whether C8 can hit ≤ 5 min.
