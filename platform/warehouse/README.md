# platform/warehouse — C4 DuckDB substrate

The single source of truth for the analytical warehouse: one DuckDB file, one
connection helper, one env var. Every other Component A layer consumes these
helpers and never hardcodes a path or re-opens DuckDB on its own.

## Files

| File            | Purpose                                                              |
| --------------- | ------------------------------------------------------------------- |
| `paths.py`      | Resolves the warehouse target. Only place the default path literal exists. |
| `connection.py` | Opens the warehouse (read/write for writers, read-only for readers).|
| `__init__.py`   | Re-exports `connect`, `connect_read_only`, `warehouse_path[_str]`. Also re-exports the stdlib `platform` surface (see note below). |

## The one env var

`DUCKDB_DATABASE` — used **everywhere** (Dagster, dbt `profiles.yml`, MCP config).

- Default: `<repo>/platform/warehouse/warehouse.duckdb` (gitignored, with `*.duckdb.wal`).
- MotherDuck escape hatch: set `DUCKDB_DATABASE=md:<db>` to dissolve single-writer.
- Legacy names `DUCKDB_PATH` and `WAREHOUSE_DB_PATH` are **rejected** and raise.

## Usage

```python
from platform.warehouse import connect, connect_read_only, warehouse_path_str

# Writers (C2 ingest, C3 dbt) — read/write, serialized, never concurrent:
with_conn = connect()                 # read/write
# Readers (C5 MCP, C4h harness, C8 probe) — concurrent-safe:
ro = connect_read_only()              # access_mode=READ_ONLY

print(warehouse_path_str())           # resolved target string
```

## Concurrency

DuckDB is single-writer-process. Writers open read/write **briefly** and are
serialized by the orchestration invariant (**ingest THEN dbt**). Readers open
`READ_ONLY` and may run concurrently with each other and with a single writer.

## Namespacing

Layers are DuckDB **schemas** (`raw` / `bronze` / `silver` / `gold`) and table
names **also** carry the layer prefix — e.g. `gold.gold_orders_obt`. This file
does not create schemas; writers (C2/C3) do.

## Note on the `platform` package name

This package is literally named `platform`, which collides with Python's stdlib
`platform` module. `platform/__init__.py` loads the genuine stdlib module and
re-exports its public surface so transitive importers (`attr` -> `dbt` ->
`dagster`, `uvicorn`, ...) keep working while `import platform.warehouse...`
still resolves to this package.
