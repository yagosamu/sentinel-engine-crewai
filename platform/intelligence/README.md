# platform/intelligence — C5 read model over gold

The serving half of Component A. A FastAPI REST facade plus a mounted FastMCP
server that expose the DuckDB **gold** layer to LLM hosts and dashboards as three
typed analytical tools — **read-only, allowlisted, and curated-intent only**. The
LLM never hands us SQL.

> Contributor handbook for `intelligence/`. Package-wide context (the asset graph,
> where gold comes from, the env contract) lives in [`../README.md`](../README.md);
> the gold models C5 reads are owned by C3 ([`../transform/README.md`](../transform/README.md)).
> Both are referenced here, not duplicated.

---

## WHAT — three tools, two transports, one engine

The three tools are implemented once in [`query_engine.py`](query_engine.py) and
surfaced over two transports that share that one engine (so behaviour and the
security boundary are identical everywhere):

| Tool | Does | Backed by |
|------|------|-----------|
| `get_schema_info` | Reflect the exposed gold tables (columns, types, live row counts) + the catalog of intents/reports. Call this first. | `information_schema` + the catalog |
| `execute_analytical_query` | Run **one curated intent** with bound params, an optional clamped `limit`, and a whitelisted `order_by`. Returns deterministic rows + the resolved SQL. | the 5 intents |
| `generate_report` | Assemble a named multi-section report; each section is an intent run sharing one param window. | the 2 reports |

| File | Role |
|------|------|
| [`gold_catalog.py`](gold_catalog.py) | The exposure boundary: `GOLD_TABLES` allowlist, 5 `Intent` definitions (`$param` templates), 2 `ReportDef`s, `DEFAULT_LIMIT`/`MAX_LIMIT`. |
| [`query_engine.py`](query_engine.py) | The three tool behaviours: param binding, `order_by` whitelist, limit clamp, JSON value coercion. Raises `IntentError`. |
| [`sql_guard.py`](sql_guard.py) | `assert_read_only_select`: comment/literal stripping + forbidden-verb scan — the second independent write-block layer. Raises `UnsafeSQLError`. |
| [`tools.py`](tools.py) | `TOOL_SCHEMAS` (hand-authored JSON Schemas — the MCP contract) + `build_mcp` (FastMCP registration). |
| [`app.py`](app.py) | `create_app()` — FastAPI `/api` + `/healthz`, with the MCP streamable-http app mounted at `/mcp`; DNS-rebinding transport security. |
| [`server.py`](server.py) | `main()` — stdio (default, for Claude Desktop) / `--http` (streamable-http) entrypoint. |

---

## WHY — the security model (allowlist by construction, two write blocks)

C5's job is to be a **read model that cannot be turned into a write or an
exfiltration channel, and cannot reach beyond gold**. Four guarantees, layered:

1. **The warehouse is opened `read_only=True`** (`connect_read_only`). A write
   physically cannot happen. Sufficient on its own to block writes.
2. **Every statement passes `assert_read_only_select`** — single statement, leads
   with `SELECT`/`WITH`, no DML/DDL/PRAGMA/ATTACH/COPY/CALL verb, no `COPY ... TO`
   exfiltration. Comments and string literals are stripped on a *separate*
   inspection copy, so `where status = 'cancelled'` does not trip the scan while
   the real literal still reaches DuckDB. Also sufficient on its own to block
   writes.
3. **No path accepts raw SQL.** `execute_analytical_query` only runs the static
   `$param` templates in `gold_catalog`, every one of which references **only**
   allowlisted gold relations; user input is bound as DuckDB `$param` values,
   never interpolated. This — the templates, *not* the guard — is what confines
   reads to the gold allowlist.
4. **`ORDER BY` identifiers come from a per-intent whitelist**, never user text;
   results are clamped to `MAX_LIMIT`.

> **Keep this distinction true:** layers 1–2 block **writes**; the **allowlist** is
> upheld by layer 3 (the templates). The SQL guard does *not* parse or restrict
> which relations a SELECT reads. If a raw-SQL intent is ever added, the allowlist
> guarantee would need its own enforcement.

Why an allowlist and not "ask DuckDB for every gold table"? Because the allowlist
is the exposure boundary the intents are authored against — a future gold model
(PII rollups, internal audit tables) stays invisible to the LLM as long as no
intent references it. Allowlist > denylist. Adding a table to `GOLD_TABLES` is a
deliberate act of exposure.

Determinism: the same intent + params over the same gold returns the same rows, so
C5 output is reproducible (R5 — Component A is verified by assertion).

---

## HOW — running C5

From the repo root (the env contract — `PYTHONPATH`, `DUCKDB_DATABASE` — is in
[`../README.md`](../README.md); gold must be materialized first via the pipeline):

```bash
# MCP over stdio — Claude Desktop and other local LLM hosts (default):
uv run python -m platform.intelligence.server

# MCP + REST over HTTP — dashboards, the eval harness, smoke tests:
uv run python -m platform.intelligence.server --http     # :8000, MCP at /mcp
uv run uvicorn platform.intelligence.app:app             # FastAPI: /api, /healthz, /mcp
```

A Claude Desktop `mcpServers` entry (full snippet in
[`server.py`](server.py)):

```json
{
  "mcpServers": {
    "ecommerce-intelligence": {
      "command": "uv",
      "args": ["run", "python", "-m", "platform.intelligence.server"]
    }
  }
}
```

### REST endpoints (the same three tools)

| Method + path | Tool |
|---------------|------|
| `GET /healthz` | liveness |
| `GET /api/tools` | the JSON-Schema tool contract (what an MCP client discovers) |
| `GET /api/schema?table=` | `get_schema_info` |
| `POST /api/query` | `execute_analytical_query` (`{intent, params, limit, order_by}`) |
| `POST /api/report` | `generate_report` (`{report, params}`) |
| `GET /api/intents` | intent + report names |

Domain errors (`IntentError`, `UnsafeSQLError`) map to clean HTTP 400s, never 500s.

### Transport security

DNS-rebinding protection is **ON by default** (the secure default for a localhost
MCP server). Expose extra hosts/origins explicitly via `C5_MCP_ALLOWED_HOSTS` /
`C5_MCP_ALLOWED_ORIGINS` (comma-separated). The in-process TestClient host
`testserver` is always allowed so the HTTP mount is testable without weakening the
deployed default.

---

## WHERE — verifying C5

C5 is verified by assertion (R5): `tests/test_c5_intelligence.py`. One reconciliation
test (`test_declared_schemas_match_registered`) asserts the hand-authored
`TOOL_SCHEMAS` and the FastMCP-derived `inputSchema` never silently drift.

---

## Conventions specific to this package

- **The catalog is the boundary.** Add a gold relation to `GOLD_TABLES` only when
  you intend to expose it, and only with an intent that references it.
- **No raw SQL, ever.** New analytical questions are new `Intent`s with `$param`
  placeholders and an `order_by_whitelist`, not a passthrough.
- **Two write blocks stay independent.** Keep both `read_only=True` and the SQL
  guard — neither is redundant; each is a full block on its own.
- **Money is a string.** `_jsonable` keeps `Decimal` as a string (no float
  rounding) and dates as ISO; preserve that when extending the engine.
