"""C5 intelligence proof tests: the three tools return real gold rows.

These exercise the actual read path against the shared warehouse's ``gold``
schema. They require gold to be materialized (``dbt build`` over a seeded +
ingested warehouse). When gold is absent the module is skipped rather than
failing, so a clean checkout without a built warehouse stays green.

Verification model (R5, Component A = verify by assertion):
  * each tool returns a NON-EMPTY result set drawn from real gold tables;
  * row counts / sums tie back to a direct DuckDB query (the engine is not
    inventing numbers);
  * the read-only guard rejects every non-SELECT;
  * the FastAPI facade returns the same rows over HTTP (TestClient).
"""

from __future__ import annotations

from platform.intelligence import query_engine as qe
from platform.intelligence import tools as c5_tools
from platform.intelligence.app import create_app
from platform.intelligence.sql_guard import UnsafeSQLError, assert_read_only_select
from platform.warehouse.connection import connection

import pytest
from fastapi.testclient import TestClient


def _gold_ready() -> bool:
    try:
        with connection(read_only=True) as conn:
            n = conn.execute(
                "select count(*) from information_schema.tables "
                "where table_schema = 'gold' and table_name = 'gold_orders_obt'"
            ).fetchone()[0]
            if not n:
                return False
            rows = conn.execute("select count(*) from gold.gold_orders_obt").fetchone()[0]
            return rows > 0
    except Exception:  # noqa: BLE001 - missing warehouse file => skip
        return False


pytestmark = pytest.mark.skipif(
    not _gold_ready(),
    reason="gold not materialized; run dbt build over a seeded+ingested warehouse to enable C5 tests",
)


@pytest.fixture(scope="module")
def client() -> TestClient:
    # TestClient enters the app lifespan, so the mounted MCP session manager runs.
    with TestClient(create_app()) as c:
        yield c


# --- tool 1: get_schema_info ----------------------------------------------


def test_get_schema_info_reflects_real_gold_tables() -> None:
    info = qe.get_schema_info()
    names = {t["name"] for t in info["tables"]}
    assert {"gold_orders_obt", "gold_revenue_daily"} <= names

    obt = next(t for t in info["tables"] if t["name"] == "gold_orders_obt")
    assert obt["materialized"] is True
    assert obt["row_count"] > 0
    col_names = {c["name"] for c in obt["columns"]}
    # Contract columns the intents depend on.
    assert {"order_id", "total_amount", "status", "customer_country"} <= col_names

    # The catalog of callable intents/reports is advertised.
    assert {"revenue_by_period", "top_products"} <= {i["name"] for i in info["intents"]}
    assert "executive_summary" in {r["name"] for r in info["reports"]}


# --- tool 2: execute_analytical_query -------------------------------------


def test_execute_query_returns_real_rows_matching_direct_sql() -> None:
    res = qe.execute_analytical_query(intent="order_status_breakdown")
    assert res["row_count"] > 0
    assert "status" in res["columns"]

    # Tie back to a direct query: the engine's order_count per status must equal
    # a raw GROUP BY against the gold table.
    engine_by_status = {r["status"]: r["order_count"] for r in res["rows"]}
    with connection(read_only=True) as conn:
        direct = dict(
            conn.execute(
                "select status, count(*) from gold.gold_orders_obt group by status"
            ).fetchall()
        )
    assert engine_by_status == direct


def test_execute_query_with_params_limit_and_order_by() -> None:
    res = qe.execute_analytical_query(
        intent="top_products",
        params={},
        limit=5,
        order_by="gross_revenue desc",
    )
    assert res["row_count"] == 5
    revenues = [float(r["gross_revenue"]) for r in res["rows"]]
    # Honoured the requested descending sort.
    assert revenues == sorted(revenues, reverse=True)
    # Honoured the limit clamp wiring.
    assert res["limit"] == 5


def test_execute_query_rejects_unknown_intent_and_param() -> None:
    with pytest.raises(qe.IntentError):
        qe.execute_analytical_query(intent="definitely_not_an_intent")
    with pytest.raises(qe.IntentError):
        qe.execute_analytical_query(intent="top_products", params={"bogus": 1})
    with pytest.raises(qe.IntentError):
        qe.execute_analytical_query(intent="top_products", order_by="order_id; drop table x")


# --- tool 3: generate_report ----------------------------------------------


def test_generate_report_assembles_real_sections() -> None:
    report = qe.generate_report(report="executive_summary")
    assert report["section_count"] == 4
    titles = {s["title"] for s in report["sections"]}
    assert "Revenue by Country" in titles
    # Every section produced real rows from gold.
    for section in report["sections"]:
        assert section["row_count"] > 0, f"empty section: {section['key']}"
        assert section["rows"], f"no rows in section: {section['key']}"


# --- read-only enforcement (the security boundary) ------------------------


@pytest.mark.parametrize(
    "bad_sql",
    [
        "drop table gold.gold_orders_obt",
        "delete from gold.gold_orders_obt",
        "update gold.gold_orders_obt set status = 'x'",
        "insert into gold.gold_orders_obt values (1)",
        "select 1; drop table gold.gold_orders_obt",
        "copy (select 1) to 'out.csv'",
        "attach 'evil.db' as evil",
        "pragma database_list",
        "create table t as select 1",
    ],
)
def test_sql_guard_rejects_non_select(bad_sql: str) -> None:
    with pytest.raises(UnsafeSQLError):
        assert_read_only_select(bad_sql)


def test_sql_guard_allows_select_and_with() -> None:
    assert_read_only_select("select 1")
    assert_read_only_select("with x as (select 1 as a) select a from x")
    # A keyword inside a string literal must NOT trip the guard.
    assert_read_only_select("select * from gold.gold_orders_obt where status = 'cancelled'")


def test_connection_is_read_only() -> None:
    # The reader handle physically cannot write — independent of the guard.
    import duckdb

    with connection(read_only=True) as conn, pytest.raises(duckdb.Error):
        conn.execute("create table gold.should_not_exist (x int)")


# --- FastAPI facade proves the same rows over HTTP ------------------------


def test_http_schema_endpoint(client: TestClient) -> None:
    resp = client.get("/api/schema")
    assert resp.status_code == 200
    body = resp.json()
    assert {"gold_orders_obt", "gold_revenue_daily"} <= {t["name"] for t in body["tables"]}


def test_http_query_endpoint_returns_rows(client: TestClient) -> None:
    resp = client.post(
        "/api/query",
        json={"intent": "revenue_by_category", "limit": 5, "order_by": "gross_revenue desc"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["row_count"] > 0
    assert body["rows"]
    assert "product_category" in body["columns"]


def test_http_report_endpoint_returns_sections(client: TestClient) -> None:
    resp = client.post("/api/report", json={"report": "executive_summary"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["section_count"] == 4
    assert all(s["row_count"] > 0 for s in body["sections"])


def test_http_query_bad_intent_is_400(client: TestClient) -> None:
    resp = client.post("/api/query", json={"intent": "nope"})
    assert resp.status_code == 400


def test_http_tools_contract_lists_three_tools(client: TestClient) -> None:
    resp = client.get("/api/tools")
    assert resp.status_code == 200
    names = {t["name"] for t in resp.json()["tools"]}
    assert names == {"get_schema_info", "execute_analytical_query", "generate_report"}


# --- the mounted /mcp streamable-http server speaks the protocol -----------


def _extract_json(text: str) -> dict | None:
    import json

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    try:
        return json.loads(text)
    except ValueError:
        return None


def test_mcp_http_mount_lists_and_calls_tools(client: TestClient) -> None:
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    init = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pytest", "version": "0"},
        },
    }
    r = client.post("/mcp/mcp", json=init, headers=headers)
    assert r.status_code == 200
    sid = r.headers.get("mcp-session-id")
    h2 = dict(headers)
    if sid:
        h2["mcp-session-id"] = sid
    client.post("/mcp/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers=h2)

    tl = client.post("/mcp/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, headers=h2)
    assert tl.status_code == 200
    listed = _extract_json(tl.text)
    assert listed is not None
    names = {t["name"] for t in listed["result"]["tools"]}
    assert names == {"get_schema_info", "execute_analytical_query", "generate_report"}

    call = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "execute_analytical_query",
            "arguments": {"intent": "revenue_by_category", "limit": 2, "order_by": "gross_revenue desc"},
        },
    }
    cr = client.post("/mcp/mcp", json=call, headers=h2)
    assert cr.status_code == 200
    called = _extract_json(cr.text)
    assert called is not None
    structured = called["result"]["structuredContent"]
    result = structured.get("result", structured)
    assert result["row_count"] == 2
    assert result["rows"]


# --- the declared JSON Schemas match what FastMCP actually registered ------


@pytest.mark.anyio
async def test_declared_schemas_match_registered() -> None:
    """The hand-authored TOOL_SCHEMAS must reconcile with FastMCP's registry."""
    mcp = c5_tools.build_mcp()
    registered = {t.name: t for t in await mcp.list_tools()}
    assert set(registered) == set(c5_tools.TOOL_SCHEMAS)
    for name, decl in c5_tools.TOOL_SCHEMAS.items():
        tool = registered[name]
        # required fields declared by the contract are required in the live schema.
        live_required = set(tool.inputSchema.get("required", []))
        declared_required = set(decl["inputSchema"].get("required", []))
        assert declared_required <= live_required, (name, declared_required, live_required)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
