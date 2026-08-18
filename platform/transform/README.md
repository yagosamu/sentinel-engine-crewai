# platform/transform — C3 dbt medallion

The refinement half of Component A. A dbt-duckdb project that reads the
defect-faithful `raw.raw_*` tables C2 lands in the warehouse and builds the
medallion — `raw` → bronze → silver (+ `*_rejects`) → gold — in the **same**
DuckDB file. C3 is the **sole writer** of the `bronze`, `silver`, and `gold`
schemas; it never touches Postgres.

> Contributor handbook for `transform/`. Package-wide context (the asset graph,
> the join seam, the env contract, running the pipeline) lives in
> [`../README.md`](../README.md); the locked decisions this layer rests on
> (Dagster-owns-raw, quarantine-not-drop, the one-DAG wiring) are recorded in
> [`../../docs/adrs/0001-analytical-backbone.md`](../../docs/adrs/0001-analytical-backbone.md).
> Both are referenced here, not duplicated.

---

## WHAT — 14 models, 5 macros, one quarantine contract

The project is `ecommerce_transform` (see [`dbt_project.yml`](dbt_project.yml)).
Layers map to **literal** DuckDB schemas (`bronze`/`silver`/`gold`), and the
layer prefix is also kept in every table name (`gold.gold_orders_obt`).

| Layer | Models | Materialization | Role |
|-------|--------|-----------------|------|
| bronze | `bronze_customers`, `bronze_products`, `bronze_orders`, `bronze_payments` | view | 1:1 typed pass-through over `source('raw', 'raw_*')`. Defects ride through verbatim — bronze does not judge. |
| silver (accepted) | `silver_customers`, `silver_products`, `silver_orders`, `silver_payments` | incremental (append) | Only rows that PASS classification. Flows to gold. |
| silver (rejects) | `silver_customers_rejects`, `silver_products_rejects`, `silver_orders_rejects`, `silver_payments_rejects` | table | Quarantine. Every rejected row, kept forever, tagged `reject_rule`/`reject_reason`/`rejected_at`. |
| gold | `gold_orders_obt`, `gold_revenue_daily` | table | Read-hot OBTs over accepted rows only — clean by construction. The AC-2 latency surface and the C5 exposure boundary. |

Macros ([`macros/`](macros)):

| Macro | Role |
|-------|------|
| [`classify.sql`](macros/classify.sql) | **The single source of truth for accept/reject per entity.** Emits every bronze row once, decorated with `reject_rule` (NULL == accepted) + `reject_reason`. Accepted models keep `where reject_rule is null`; rejects keep the complement. |
| [`build_rejects.sql`](macros/build_rejects.sql) | Renders a `*_rejects` table body from a `classify_*()` call: keeps rejected rows, surfaces the source columns verbatim, stamps `rejected_at`. |
| [`incremental_quarantine.sql`](macros/incremental_quarantine.sql) | The `_ingested_at` watermark machinery (`evict_reprocessed_window` pre-hook + `append_only_new` body filter) that keeps the incremental path from leaking a stale-clean row into gold. |
| [`enums.sql`](macros/enums.sql) | The known `ORDER_STATUSES` / `PAYMENT_STATUSES` domains (mirrors `src/seed/factories.py`) + `sql_in_list`. A status outside the domain is `malformed_data`. |
| [`generate_schema_name.sql`](macros/generate_schema_name.sql) | Maps a model's `+schema` to that **literal** DuckDB schema (not dbt's default `<target>_<custom>`), so layers land in `bronze`/`silver`/`gold` exactly. |

The only property file is [`models/bronze/sources.yml`](models/bronze/sources.yml):
it declares the `raw` source and **intentionally carries no tests** — raw is
supposed to contain defects.

---

## WHY — quarantine, not drop (the U3 detection seam)

A dropped defect is invisible; a quarantined defect is evidence. Silver routes
every rejected row into `silver_<entity>_rejects` rather than discarding it, and
the `reject_rule` is the **machine key == the generator's `failure_key`** — so an
eval (and the future Sentinel) can join
`silver_<entity>_rejects.reject_rule → injected_incidents.failure_key`. Gold is
built over accepted rows only, so it is clean *by construction* without losing
the audit trail. This is the locked answer to U3: **the defect is visible in
silver, absent from gold.**

The accept/reject split holds **by construction**: accepted and rejects both
consume the *same* `classify_*()` output, so `accepted + rejects == bronze` for
every entity — no row can be both, none can be silently dropped.

What is accepted-and-flagged rather than rejected (per the failure map):

- `late_arrival` → `is_late` flag on `silver_orders` (a backdated row is anomalous
  but valid; rejecting it would be a bug).
- `volume_spike` / `ambiguous_anomaly` → valid rows; a count/state signal, not a
  row defect.
- `schema_drift` → already normalized into the stable column by C2; `_schema_drift`
  passes through as a flag.

### The incremental-quarantine watermark (the subtle part)

On the default **incremental** (non-full-refresh) path, a row can transition
*accepted → rejected in place* (`destructive_fix` and `malformed_data` UPDATE
existing rows). dbt's stock delete+insert only deletes keys still present in the
model's SELECT, so a now-rejected key — absent from the accepted SELECT — would
never be deleted and its **stale clean row would survive in gold**. That is the
exact U3 leak the incremental path must not have.

The fix uses the **processing-time** watermark `_ingested_at` (C2 re-stamps it on
every raw upsert — brand-new PK and in-place overwrite alike):

1. `evict_reprocessed_window` (a `pre_hook`) DELETEs from the accepted table every
   `unique_key` whose bronze row now carries a strictly newer `_ingested_at` —
   i.e. it was re-extracted this cycle (accepted or now-rejected; the pre-hook
   does not care about classification).
2. The model body then APPENDs only the **accepted** re-extracted rows
   (`append_only_new` anti-join). A now-rejected re-extracted row was evicted and
   is **not** re-appended — it lands in `*_rejects` instead.

The comparison is per-key (no global scalar watermark), so concurrent deletes can
never lower a boundary the body relies on. First build / `--full-refresh` makes
the pre-hook a safe no-op. See
[`models/silver/silver_orders.sql`](models/silver/silver_orders.sql) for the
worked case.

---

## HOW — building the medallion

C3 is normally run as part of the **one connected Dagster asset graph**, not
standalone — `backbone_end_to_end` runs C2 ingest then this dbt build in order,
honoring DuckDB single-writer. See [`../README.md`](../README.md) for the full
pipeline. From the repo root:

```bash
make dbt-build      # raw -> bronze/silver/gold (the medallion only)
make ingest-once && make dbt-build   # the two writers, in the required order
```

Direct dbt invocation (the env var pins the shared warehouse file; the profile
forces `threads: 1` for single-writer):

```bash
DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb \
  uv run dbt build --project-dir platform/transform \
                   --profiles-dir platform/transform/profiles
```

> **Single-writer.** Never run `dbt build` while C2 ingest, the C8 probe, or the
> defect eval is writing the warehouse. A "database is locked" means a stale
> holder is open — clear it with
> `lsof -t platform/warehouse/warehouse.duckdb | xargs -r kill -9`.

---

## WHERE — verifying C3

C3 is verified by assertion (R5), through two surfaces:

- **`platform/evals/eval_defect_survival.sh`** — the U3 gate: inject a defect →
  C2 → dbt → assert it is **caught** in the right `silver_*_rejects` table AND
  **absent** from `gold_orders_obt`. See [`../evals/README.md`](../evals/README.md).
- **`platform/tests`** — the end-to-end backbone tests (`test_e2e_backbone.py`,
  `test_e2e_incremental_medallion.py`) exercise the full and incremental paths.

> **Note on dbt data tests.** `dbt build` *would* run dbt data tests, but this
> project currently declares **none** — there is no `tests/` directory and no
> `schema.yml` test blocks (only `sources.yml`, which intentionally has none). So
> `dbt build` runs **models only** today, and the `tests: +store_failures: true`
> config on gold in `dbt_project.yml` is **inert** until tests are added.
> Data-quality enforcement currently lives in the **quarantine** mechanism and is
> verified by the evals above, not by dbt `schema.yml` tests. Adding dbt tests is
> a known, deliberate extension point — this README describes what exists.

---

## Conventions specific to this package

These extend (don't replace) [`../README.md`](../README.md) and the repo rules:

- **Bronze does not judge; silver classifies; gold is clean by construction.** If
  you find yourself filtering a defect out in bronze, it belongs in `classify.sql`
  (which routes it to `*_rejects`) instead.
- **`reject_rule` IS the generator `failure_key`.** When adding a classifier
  branch, the rule string must equal the failure key in
  `src/gen/failures.py`, or the eval join breaks.
- **First-match-wins in `classify_orders`.** A row corrupted two ways is attributed
  to the higher-priority defect; rule order is load-bearing — read the comment
  block before reordering.
- **Schemas are literal.** Don't remove `generate_schema_name.sql` — without it dbt
  would build `<target>_bronze` etc. and break the `gold.gold_*` namespacing C4/C5
  depend on.
