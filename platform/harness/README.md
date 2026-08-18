# C4h — Peak-Load Harness (AC-1 Gate)

Proves **AC-1: isolation under peak load** — that running an analytics read load
against the source Postgres causes **no analytics-attributable lock-wait on the
transactional (OLTP) path**, even while the database is under a
75k-orders/day-equivalent insert load.

This is a *measured* gate, not an asserted one: it drives real concurrent load,
samples the server's lock graph, attributes every lock-wait by
`application_name`, and emits a pass/fail verdict with a non-zero exit code.

## How it works

```
                 ┌────────────────────────────────────────────┐
                 │  Postgres (source: ecommerce)              │
                 └────────────────────────────────────────────┘
   oltp_writer x N   ──INSERT orders (peak, unthrottled)──▶  │  ◀── the path AC-1 protects
   dagster_ingest x M ──analytical SELECTs (read-only)───▶   │  ◀── the analytics surface
   c4h_monitor x 1   ──poll pg_blocking_pids / pg_locks──▶   │  ◀── the attribution observer
```

* **Transactional path** — writer sessions tagged `application_name='oltp_writer'`
  insert FK-valid orders in small batches (one commit per batch) at peak
  saturation. Per-commit latency (p50/p95/p99/max) is recorded.
* **Analytics path** — reader sessions tagged `application_name='dagster_ingest'`
  (the contract's fleet-wide analytics attribution name) run representative
  group-by / join / range-scan queries, read-only. The orders→customers join
  column is **drift-resolved at runtime** (schema_drift may have renamed
  `orders.customer_id` → `user_id`), mirroring `src.gen`.
* **Monitor** — a background thread polls `pg_blocking_pids()` joined to
  `pg_stat_activity` and records every lock-wait edge (waiter → blocker) with
  both `application_name`s.

### The AC-1 verdict

```
PASS  iff  load_floor_met  AND  count(analytics-attributable lock-waits) <= threshold(0)
```

* **analytics-attributable** = an `oltp_writer` waiting on a lock held by a
  `dagster_ingest` session. This is the precise AC-1 failure condition.
* **load_floor_met** = achieved throughput cleared the validity floor. A clean
  result under *no* load proves nothing, so an under-loaded run is reported
  **INCONCLUSIVE**, not PASS.
* Writer-vs-writer contention is real but **not** analytics-attributable, so it
  does not fail AC-1 (that's the entire point of attribution).

## Running

```bash
# CI / fast sanity (default): peak-saturation code path, ~4s window
make ac1
#   or directly:
uv run python -m platform.harness.cli --profile smoke

# The real gate: 75k/day-equivalent floor, 60s window, real concurrency
make ac1-full
#   or:
uv run python -m platform.harness.cli --profile full

# JSON report (for evals / dashboards)
uv run python -m platform.harness.cli --profile full --json

# Override the window
uv run python -m platform.harness.cli --profile smoke --duration 8
```

### Exit codes (the gate)

| code | meaning |
|------|---------|
| 0 | AC-1 **PASS** — peak load applied and no analytics-attributable lock-wait |
| 1 | AC-1 **FAIL** — an OLTP writer was blocked by an analytics session |
| 2 | precondition error — Postgres unreachable or unseeded (could not run) |
| 3 | AC-1 **INCONCLUSIVE** — load floor not met (not a valid peak-load test) |

## Preconditions

* Postgres up and reachable (`make up`) and seeded (`make seed`) — writers need
  FK-valid `customers`/`products`.
* Connection details come from `src.db.connection.conninfo()` (env
  `POSTGRES_HOST/PORT/DB/USER/PASSWORD`).

> **Note:** this harness writes real `orders` rows into the source DB as its
> load. That is intentional (it IS the peak load). Re-seed with `make reseed` if
> you want a pristine source afterwards.

## Profiles

Defined in `config.py`:

| | `smoke` | `full` |
|---|---|---|
| window | ~4s | 60s |
| oltp writers | 2 | 4 |
| analytics readers | 2 | 3 |
| load floor | 10k/day-equiv | 75k/day-equiv |
| sample interval | 0.1s | 0.25s |

Both share the identical code path, attribution logic, and verdict shape — they
differ only in scale/duration.

## Tests

```bash
uv run pytest tests/harness -v
```

* `test_verdict.py` — unit tests of the decision rule (no DB).
* `test_lockwait_detection.py` — integration tests against a live Postgres,
  including a **true-positive** test that injects a real analytics-held lock and
  asserts the monitor flags it (a gate that can only say PASS is worthless).
  These skip cleanly when Postgres is unreachable.

## Files

| file | role |
|------|------|
| `config.py` | profiles + the 75k/day target and attribution app-name constants |
| `pg_session.py` | tagged Postgres sessions (the AC-1 `application_name` linchpin) |
| `loadgen.py` | OLTP writer + analytics reader workers; latency capture |
| `monitor.py` | background lock-wait sampler + attribution |
| `verdict.py` | AC-1 decision rule + structured/text report |
| `runner.py` | orchestrates one run (workers + monitor + window) |
| `cli.py` | entrypoint; profile selection; gate exit code |
