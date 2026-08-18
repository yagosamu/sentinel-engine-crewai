"""C5 FastAPI app: REST facade + mounted MCP streamable-http server.

Two ways to reach the same three tools over the same read-only gold model:

* **MCP (LLM hosts):** the FastMCP streamable-http app is mounted at ``/mcp``.
  Claude Desktop / any MCP client speaks the protocol there.
* **REST (humans, dashboards, the C4h/eval harness, smoke tests):** thin JSON
  endpoints under ``/api`` call the identical :mod:`query_engine` functions.

Both paths share one engine, so there is exactly one behaviour and one security
boundary. The app opens NO writable handle and imports nothing from Postgres.

Mounting follows the MCP Python SDK pattern: the FastMCP session manager must run
inside the host app's lifespan, so we drive it from the FastAPI ``lifespan``.
"""

from __future__ import annotations

import contextlib
import os
from platform.intelligence import gold_catalog as cat
from platform.intelligence import query_engine as qe
from platform.intelligence.sql_guard import UnsafeSQLError
from platform.intelligence.tools import TOOL_SCHEMAS, build_mcp
from typing import Any

from fastapi import FastAPI, HTTPException
from mcp.server.fastmcp.server import TransportSecuritySettings
from pydantic import BaseModel, Field


def _transport_security() -> TransportSecuritySettings:
    """Build the MCP HTTP transport security from env.

    DNS-rebinding protection stays ON by default (the secure default for a
    localhost MCP server). Operators expose extra hosts/origins explicitly via
    ``C5_MCP_ALLOWED_HOSTS`` / ``C5_MCP_ALLOWED_ORIGINS`` (comma-separated). The
    in-process TestClient host ``testserver`` is always allowed so the HTTP mount
    is verifiable without weakening the deployed default.
    """

    def _csv(name: str) -> list[str]:
        return [h.strip() for h in os.environ.get(name, "").split(",") if h.strip()]

    hosts = ["testserver", "localhost", "127.0.0.1", *_csv("C5_MCP_ALLOWED_HOSTS")]
    origins = _csv("C5_MCP_ALLOWED_ORIGINS")
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=origins,
    )


# One MCP server instance, shared by the mounted ASGI app and (via server.py) by
# the stdio transport.
mcp = build_mcp(
    stateless_http=True,
    json_response=True,
    transport_security=_transport_security(),
)


# --- REST request models ---------------------------------------------------


class QueryRequest(BaseModel):
    intent: str = Field(..., description="A curated intent name.")
    params: dict[str, Any] = Field(default_factory=dict)
    limit: int | None = None
    order_by: str | None = None


class ReportRequest(BaseModel):
    report: str = Field(..., description="A curated report name.")
    params: dict[str, Any] = Field(default_factory=dict)


def _handle(fn, /, **kwargs):
    """Run an engine call, mapping domain errors to clean 400s."""
    try:
        return fn(**kwargs)
    except (qe.IntentError, UnsafeSQLError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    # Run the MCP session manager for the lifetime of the FastAPI app.
    async with mcp.session_manager.run():
        yield


def create_app() -> FastAPI:
    """Build the C5 FastAPI app: REST endpoints + the mounted MCP server.

    Wires the ``/api`` JSON endpoints and ``/healthz`` to the shared
    :mod:`query_engine`, and mounts the FastMCP streamable-http app at ``/mcp``.
    The MCP session manager runs inside this app's ``lifespan`` (the SDK
    requirement). The returned app opens no writable handle — every path reads
    gold read-only.
    """
    app = FastAPI(
        title="C5 Intelligence",
        version="0.1.0",
        description="Read-only analytical tools over the DuckDB gold layer.",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/tools")
    def list_tools() -> dict[str, Any]:
        """The JSON-Schema tool contract (what an MCP client would discover)."""
        return {"tools": list(TOOL_SCHEMAS.values())}

    @app.get("/api/schema")
    def schema(table: str | None = None) -> dict[str, Any]:
        return _handle(qe.get_schema_info, table=table)

    @app.post("/api/query")
    def query(req: QueryRequest) -> dict[str, Any]:
        return _handle(
            qe.execute_analytical_query,
            intent=req.intent,
            params=req.params,
            limit=req.limit,
            order_by=req.order_by,
        )

    @app.post("/api/report")
    def report(req: ReportRequest) -> dict[str, Any]:
        return _handle(qe.generate_report, report=req.report, params=req.params)

    @app.get("/api/intents")
    def intents() -> dict[str, Any]:
        return {
            "intents": [i.name for i in cat.INTENTS.values()],
            "reports": [r.name for r in cat.REPORTS.values()],
        }

    # Mount the MCP streamable-http server. Its own session manager is driven by
    # this app's lifespan above.
    app.mount("/mcp", mcp.streamable_http_app())
    return app


app = create_app()
