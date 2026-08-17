# `src/` — Source Layer (Component A, Layer 1)

The **source of record** for the whole platform: the operational e-commerce
Postgres database, a deterministic seeder that fills it with a clean baseline,
and a 14-mode chaos generator that injects failures and logs ground truth.

This layer is the only thing in the repo that **writes** to source Postgres.
Everything downstream (Dagster ingestion, dbt medallion, DuckDB, Sentinel)
reads what this layer produces. The `injected_incidents` table is the
**architectural seam**: the generator writes it (ground truth); the future
Sentinel reads it read-only as its scoring oracle (interface I4).

```
src/
├── db/      schema + connection/bulk helpers   (the database itself)
├── seed/    deterministic clean baseline        (factories + seeder CLI)
└── gen/     chaos generator                      (14 failures + traffic + ledger)
```

All three packages are driven through the **Makefile** at the repo root — that
is the public entrypoint. `make help` lists every target.

---

## `src/db` — schema and connection

| File | Role |
|------|------|
| `01_schema.sql` | The full source schema (5 tables). Mounted into the Postgres container at `/docker-entrypoint-initdb.d`, so it auto-applies on **first boot** (`make up`). |
| `connection.py` | psycopg connection factory + bulk helpers (`insert_returning_ids`, `count`, `truncate_all`). |

### Tables (`01_schema.sql`)

- **Business tables** — `customers`, `products`, `orders`, `payments`.
  `BIGINT … GENERATED ALWAYS AS IDENTITY` primary keys, FK chain
  `payments → orders → {customers, products}`, `CHECK` constraints on money and
  quantity, and four indexes on the `orders`/`payments` hot paths.
- **Ground-truth ledger** — `injected_incidents` (`failure_key`, `detail`,
  `detected_by`, `injected_at`). Not a business table: it is the oracle the
  generator appends to on every injection.

### `connection.py`

- `conninfo()` reads connection settings from `POSTGRES_*` env vars (defaults:
  `localhost:5432`, db `ecommerce`, user/pass `postgres`). No secrets in code.
- `connect()` returns a psycopg connection with **`autocommit=False`** — callers
  control transactions and must `commit()` explicitly.
- `insert_returning_ids(conn, table, columns, rows)` — bulk `executemany(...,
  returning=True)`, deriving the PK name as `table[:-1] + "_id"`. Returns the new
  IDs in insert order. This is how the seeder threads FKs across tables.
- `truncate_all(conn)` — `TRUNCATE payments, orders, products, customers RESTART
  IDENTITY CASCADE`. **Deliberately omits `injected_incidents`** so the
  ground-truth ledger survives a reseed (see R7 / test `test_e2e_backbone.py`).

---

## `src/seed` — deterministic clean baseline

Generates a correlated, referentially-valid dataset. Deterministic: seeding with
the same `--seed` produces the same rows.

| File | Role |
|------|------|
| `factories.py` | Frozen dataclasses (`Customer`/`Product`/`Order`/`Payment`) + `EcommerceFactory` that builds plausible, correlated values from Faker. |
| `seed.py` | CLI (`python -m src.seed.seed`) that wires the factory to bulk inserts. |

### Correctness properties the factory guarantees

- **Money is `Decimal`** via `_money()` (`ROUND_HALF_UP`, 2 dp) — never float in
  the database.
- **Price/cost correlation** — `cost` is 45–80% of `unit_price`; both are bounded
  by per-category price bands in `CATEGORIES`.
- **Temporal ordering** — `customer.created_at` ≤ `order.ordered_at` ≤
  `payment.paid_at` (each `date_time_between` is bounded by the prior event).
- **Status correlation** — a `returned` order forces `payment.status =
  'refunded'`.
- **Uniqueness** — `email` and `sku` use `faker.unique`.

### Seeder flow (`seed.py::run`)

1. `Faker.seed(seed)` for reproducibility.
2. Optional `truncate_all` (`--truncate`).
3. Insert customers → get IDs → insert products → get IDs (+ unit_price catalog).
4. For each order, pick a random customer/product, build the `Order` with
   `not_before=customer.created_at`, bulk-insert, get order IDs.
5. Build one payment per order, bulk-insert.
6. `commit()`, then return per-table counts.

**CLI:** `--customers` (500) · `--products` (200) · `--orders` (5000) ·
`--seed` (42) · `--truncate`. Run via `make seed` / `make reseed`.

---

## `src/gen` — chaos generator

Streams normal traffic and injects failures into source Postgres, recording each
injection to `injected_incidents`. CLI is `python -m src.gen.cli` (`make
traffic` / `inject` / `watch` / `reset-schema`).

| File | Role |
|------|------|
| `failures.py` | The 14-failure `REGISTRY`. Each `Failure` declares `key`, `summary`, `detected_by`, `unlocks` and an `inject(conn)` method returning an `InjectionResult`. |
| `repository.py` | All SQL the generator needs — sampling, inserts, the schema-drift–aware `order_customer_column`, and `record_incident`/`count_incidents`. |
| `engine.py` | `TrafficGenerator` (normal orders), `inject()` (one failure + record + commit), `watch()` (continuous loop). |
| `cli.py` | `argparse` front-end: `list`, `traffic`, `inject`, `reset-schema`, `watch`. |

### Schema-drift awareness (the subtle part)

The `schema_drift` failure renames `orders.customer_id → user_id`. To stay
correct **after** drift, every generator query resolves the live column name via
`repository.order_customer_column(conn)` (queries `information_schema`, defaults
to `customer_id`). This is why injectors and traffic build their column lists
dynamically instead of hardcoding `customer_id`.

### Constraint handling

Several injectors call `_disable_order_checks(conn)` to `DROP CONSTRAINT IF
EXISTS` the `orders` CHECK/NOT NULL guards so bad rows can land. This is **not
restored** by the generator — only `reset-schema` reverts the column rename, so a
full clean baseline requires `make reset` (drop the volume and re-apply
`01_schema.sql`). See *Gaps & cautions* and rule R7.

### The 14-failure registry

`make failures` (`gen list`) splits these into **base crew** (the 4-capability
detect/diagnose/report core) and **feature-unlocking** (each forces one CrewAI
capability). The `unlocks` text below is copied verbatim from `failures.py`.

#### Base-crew failures (detect / diagnose / report)

| key | detected_by | what it injects |
|-----|-------------|-----------------|
| `negative_price` | Data Profiler | Order with negative `unit_price`/`total`. |
| `missing_customer` | Data Profiler | Order with `NULL` customer (orphan). |
| `invalid_quantity` | Data Profiler | Order with `quantity = -5`. |
| `duplicate_order` | Data Profiler | Exact duplicate of the latest order row. |
| `late_arrival` | Data Profiler | Order backdated 45 days. |
| `volume_spike` | Data Profiler | Burst of 500 orders at once. |
| `schema_drift` | Log Analyst | Renames `orders.customer_id → user_id`. |
| `orphan_payment` | Data Profiler | Payment referencing `order_id=999999999`. |

#### Feature-unlocking failures (each demands a CrewAI capability)

| key | detected_by | unlocks (from `failures.py`) |
|-----|-------------|------------------------------|
| `recurring_incident` | Data Profiler | CrewAI **Memory** — recognise a repeat offender. |
| `ambiguous_anomaly` | Data Profiler | CrewAI **Knowledge/RAG** — consult a runbook to disambiguate two plausible root causes. |
| `destructive_fix` | Data Profiler | CrewAI **Human-in-the-loop** — destructive remediation must pause for approval. |
| `malformed_data` | Data Profiler | CrewAI **Guardrails + `output_pydantic`** — force a typed post-mortem. |
| `slow_source` | Log Analyst | CrewAI **tool reliability** — `max_retry`, timeouts, fallbacks (holds a lock / `pg_sleep`). |
| `multi_failure_cascade` | Manager | CrewAI **Flows + conditional routing** — fires `missing_customer` + `volume_spike` + `schema_drift` together and records each sub-incident. |

> `multi_failure_cascade` records its three sub-incidents individually **and**
> itself, so one cascade injection writes 4 `injected_incidents` rows.

---

## Control / data flow

```
make up        docker-entrypoint-initdb.d  →  01_schema.sql applied on first boot
make seed      seed.py → EcommerceFactory → insert_returning_ids → 4 business tables (committed)
make traffic   engine.TrafficGenerator.emit → bulk INSERT into orders
make inject K  engine.inject → failures.REGISTRY[K].inject(conn)
                                 → repository.record_incident → injected_incidents
                                 → conn.commit()
make watch     engine.watch loop: emit batch, every Nth tick inject a random failure
make reset-schema  cli._reset_schema → rename user_id back to customer_id (drift only)
```

Downstream consumers (out of scope for this layer) read these tables; the
Sentinel reads `injected_incidents` as ground truth (I4). Per R3, nothing here
imports or depends on downstream code.

## Running it

| Make target | Command underneath |
|-------------|--------------------|
| `make up` / `down` / `reset` | start / stop / destroy+recreate Postgres |
| `make seed` / `reseed` | `python -m src.seed.seed [--truncate]` |
| `make traffic` (`TRAFFIC=n`) | `gen traffic --orders n` |
| `make inject` (`FAILURE=key`) | `gen inject key` |
| `make failures` | `gen list` |
| `make watch` | `gen watch` (Ctrl-C to stop) |
| `make reset-schema` | `gen reset-schema` |

Connection is configured via `POSTGRES_*` env vars (see `.env` / `docker-compose.yml`).

## Gaps & cautions

- **Chaos is not self-reversing.** Injectors `DROP CONSTRAINT`, mutate rows
  (`destructive_fix`, `malformed_data`, `ambiguous_anomaly`), and rename columns
  without a restore path beyond `reset-schema` (rename only). A reproducible
  inject→detect→score run must rebuild a clean baseline first (`make reset` +
  `make seed`). This is rule **R7**. The destructive injectors and
  `_disable_order_checks` now carry inline warnings to this effect.
- **The `unlocks` fields are the live R6 map.** The failure → CrewAI-capability
  mapping that rule R6 binds the crew to currently lives in the `unlocks` strings
  on each `Failure` subclass in `failures.py` (and is summarised in this README).
  The KB reference (`kb/crewai/reference/capability-unlock-map.md`) is still a
  stub; until it is populated, treat `failures.py` as the source of truth.
- **No unit tests for `src/`.** The seeder/factories/generator are exercised only
  indirectly by repo-level e2e tests; there is no isolated test of the registry
  (14 keys) or the factory invariants (Decimal money, temporal ordering, cost
  ratio).
