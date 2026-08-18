"""The MCP tool contract: explicit JSON Schemas + FastMCP registration.

"Tools are the API. Schemas are the contract. The LLM is just the caller."

This module holds:

* :data:`TOOL_SCHEMAS` — hand-authored JSON Schemas for the three tools. These
  are the *contract* an LLM host (Claude Desktop) reads to learn how to call us.
  We author them explicitly (rather than only deriving from type hints) so the
  enum of valid intents/reports and the parameter docs are exact and stable.
* :func:`build_mcp` — registers the three tools on a :class:`FastMCP` server,
  wiring each to its :mod:`query_engine` implementation and surfacing
  :class:`IntentError` / guard failures as clean tool errors.

The schemas and the FastMCP-derived ``inputSchema`` are reconciled by a test
(``tests/test_c5_intelligence.py::test_declared_schemas_match_registered``) so
the two never silently drift.
"""

from __future__ import annotations

from platform.intelligence import gold_catalog as cat
from platform.intelligence import query_engine as qe
from typing import Any

from mcp.server.fastmcp import FastMCP

# Enumerations derived from the catalog so the schema can never list a tool the
# engine does not implement.
_INTENT_NAMES = sorted(cat.INTENTS)
_REPORT_NAMES = sorted(cat.REPORTS)
_TABLE_NAMES = sorted(cat.GOLD_TABLES)


TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "get_schema_info": {
        "name": "get_schema_info",
        "title": "Get gold-layer schema info",
        "description": (
            "Describe the exposed gold analytical tables (columns, types, live "
            "row counts) and the catalog of curated query intents and reports. "
            "Call this first to learn what you can ask for. Optionally pass a "
            "single table name to scope the reflection."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Optional: reflect just this gold table.",
                    "enum": _TABLE_NAMES,
                }
            },
            "additionalProperties": False,
        },
    },
    "execute_analytical_query": {
        "name": "execute_analytical_query",
        "title": "Execute a curated analytical query",
        "description": (
            "Run one curated, parameterized analytical intent against the gold "
            "tables and return deterministic rows. The intent maps to a fixed, "
            "read-only SELECT; you supply filter params, an optional row limit, "
            "and an optional order_by drawn from the intent's allowed columns. "
            "Raw SQL is NOT accepted — choose an intent from the enum."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "intent": {
                    "type": "string",
                    "description": "Which curated analytical question to run.",
                    "enum": _INTENT_NAMES,
                },
                "params": {
                    "type": "object",
                    "description": (
                        "Intent-specific filter values (e.g. start_date, "
                        "end_date, category). Bound as SQL parameters; never "
                        "interpolated. See get_schema_info for each intent's params."
                    ),
                    "additionalProperties": True,
                },
                "limit": {
                    "type": "integer",
                    "description": f"Max rows (1..{cat.MAX_LIMIT}, default {cat.DEFAULT_LIMIT}).",
                    "minimum": 1,
                    "maximum": cat.MAX_LIMIT,
                },
                "order_by": {
                    "type": "string",
                    "description": (
                        "Sort clause as 'column' or 'column asc|desc'. The "
                        "column must be one of the intent's sortable columns."
                    ),
                },
            },
            "required": ["intent"],
            "additionalProperties": False,
        },
    },
    "generate_report": {
        "name": "generate_report",
        "title": "Generate a multi-section analytical report",
        "description": (
            "Assemble a named, multi-section report (each section is a curated "
            "intent run) over the gold tables. Optionally pass a shared "
            "params object (e.g. a start_date/end_date window) applied to every "
            "date-aware section. Output is deterministic."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "report": {
                    "type": "string",
                    "description": "Which named report to assemble.",
                    "enum": _REPORT_NAMES,
                },
                "params": {
                    "type": "object",
                    "description": "Optional shared filters (e.g. start_date, end_date).",
                    "additionalProperties": True,
                },
            },
            "required": ["report"],
            "additionalProperties": False,
        },
    },
}


def build_mcp(name: str = "ecommerce-intelligence", **fastmcp_kwargs: Any) -> FastMCP:
    """Build a FastMCP server with the three C5 tools registered.

    The same instance is used for stdio (Claude Desktop) and for the
    streamable-http app mounted under FastAPI, so the tool surface is identical
    across transports.
    """
    mcp = FastMCP(name, **fastmcp_kwargs)

    @mcp.tool(
        name="get_schema_info",
        title=TOOL_SCHEMAS["get_schema_info"]["title"],
        description=TOOL_SCHEMAS["get_schema_info"]["description"],
    )
    def get_schema_info(table: str | None = None) -> dict[str, Any]:
        return qe.get_schema_info(table=table)

    @mcp.tool(
        name="execute_analytical_query",
        title=TOOL_SCHEMAS["execute_analytical_query"]["title"],
        description=TOOL_SCHEMAS["execute_analytical_query"]["description"],
    )
    def execute_analytical_query(
        intent: str,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        return qe.execute_analytical_query(
            intent=intent, params=params, limit=limit, order_by=order_by
        )

    @mcp.tool(
        name="generate_report",
        title=TOOL_SCHEMAS["generate_report"]["title"],
        description=TOOL_SCHEMAS["generate_report"]["description"],
    )
    def generate_report(
        report: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return qe.generate_report(report=report, params=params)

    return mcp
