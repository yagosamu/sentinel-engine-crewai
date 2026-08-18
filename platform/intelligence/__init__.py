"""C5 intelligence (FastAPI + MCP). Read-only over the ``gold`` schema ONLY.

Exposes three MCP tools (get_schema_info / execute_analytical_query /
generate_report) via stdio and streamable-http transports. Opens the warehouse
read-only through ``platform.warehouse.connect_read_only``. No Postgres import
anywhere under this package (AC-1 depends on zero source surface).

Public surface:
  * ``query_engine`` — the three tool behaviours (intent -> read-only SQL -> rows)
  * ``tools.TOOL_SCHEMAS`` / ``tools.build_mcp`` — the JSON-Schema contract + FastMCP
  * ``app.create_app`` / ``app.app`` — the FastAPI facade with the MCP mount
  * ``server.main`` — stdio / streamable-http entrypoint
"""

from __future__ import annotations

__all__ = [
    "get_schema_info",
    "execute_analytical_query",
    "generate_report",
]


def __getattr__(name: str):
    # Lazy re-export so importing the package does not eagerly pull in FastAPI/
    # DuckDB at module-import time (keeps the stdlib-shadowing __init__ light).
    if name in __all__:
        from platform.intelligence import query_engine

        return getattr(query_engine, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
