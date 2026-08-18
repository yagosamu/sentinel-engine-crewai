"""The C5 read engine: intent -> parameterized read-only DuckDB SQL -> rows.

This module owns the three tool *behaviours* (the FastMCP/FastAPI layers are thin
wrappers over the functions here):

* :func:`get_schema_info`        — reflect the exposed gold tables/columns.
* :func:`execute_analytical_query` — run a curated intent with bound params.
* :func:`generate_report`        — assemble a named multi-section report.

Hard guarantees. Two INDEPENDENT layers each fully block a WRITE (1, 2); the
remaining guarantees (3-5) define the READ surface and are upheld by the curated
intent templates, not by the guard:

1. The warehouse is opened ``read_only=True`` (``connect_read_only``); a write
   physically cannot happen. (Sufficient on its own to block writes.)
2. Every statement passes :func:`sql_guard.assert_read_only_select`, which
   rejects any non-SELECT / multi-statement / DML-DDL verb. (Also sufficient on
   its own to block writes.) NOTE: the guard blocks WRITES; it does NOT parse or
   restrict which RELATIONS a SELECT reads — the gold-table allowlist is upheld
   by guarantee 3 (the templates), not by the guard.
3. The only SQL bodies executed are the static templates in ``gold_catalog``,
   each of which references ONLY allowlisted gold relations; user input is bound
   as DuckDB ``$param`` values, never interpolated. This is what confines reads
   to the gold allowlist (no path accepts raw SQL).
4. ``ORDER BY`` identifiers come from a per-intent whitelist, never user text.
5. Results are clamped to ``MAX_LIMIT`` rows.

Determinism: the same intent + params over the same gold tables returns the same
rows, so C5 output is reproducible (R5: Component A is verified by assertion).
"""

from __future__ import annotations

from decimal import Decimal
from platform.intelligence import gold_catalog as cat
from platform.intelligence.sql_guard import assert_read_only_select
from platform.warehouse.connection import connection
from typing import Any

import duckdb


class IntentError(ValueError):
    """Raised for an unknown intent/report or invalid parameters."""


# --- value coercion --------------------------------------------------------


def _jsonable(value: Any) -> Any:
    """Coerce a DuckDB cell into a JSON-serialisable Python value.

    DuckDB returns ``Decimal`` for DECIMAL columns and ``date``/``datetime`` for
    temporal ones. Money stays a string (no float rounding); dates become ISO
    strings; everything else passes through.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, int | float | bool | str):
        return value
    return str(value)


def _rows_to_dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, (_jsonable(v) for v in row), strict=True)) for row in cur.fetchall()]


def _clamp_limit(limit: int | None) -> int:
    if limit is None:
        return cat.DEFAULT_LIMIT
    try:
        n = int(limit)
    except (TypeError, ValueError) as exc:
        raise IntentError(f"limit must be an integer, got {limit!r}") from exc
    if n <= 0:
        raise IntentError("limit must be positive")
    return min(n, cat.MAX_LIMIT)


def _resolve_order_by(intent: cat.Intent, order_by: str | None) -> str | None:
    """Validate ``order_by`` against the intent whitelist; return a safe clause.

    Accepts ``"col"`` or ``"col desc"`` / ``"col asc"``. The column MUST be in
    ``intent.order_by_whitelist`` — otherwise we never interpolate it.
    """
    if order_by is None:
        return intent.default_order_by
    parts = order_by.strip().split()
    col = parts[0]
    direction = parts[1].lower() if len(parts) > 1 else "asc"
    if col not in intent.order_by_whitelist:
        raise IntentError(
            f"order_by column {col!r} is not sortable for intent {intent.name!r}; "
            f"allowed: {', '.join(intent.order_by_whitelist)}"
        )
    if direction not in {"asc", "desc"}:
        raise IntentError(f"order_by direction must be asc/desc, got {direction!r}")
    return f"{col} {direction}"


# --- tool 1: get_schema_info ----------------------------------------------


def get_schema_info(table: str | None = None) -> dict[str, Any]:
    """Reflect the exposed gold tables (allowlist), optionally one table.

    Returns the table list with column names/types and live row counts, plus the
    catalog of curated intents and reports an LLM can call. This is the contract
    surface the model reads before composing a query.
    """
    if table is not None and table not in cat.GOLD_TABLES:
        raise IntentError(
            f"unknown gold table {table!r}; exposed tables: {', '.join(cat.GOLD_TABLES)}"
        )

    targets = (table,) if table else cat.GOLD_TABLES
    tables_info: list[dict[str, Any]] = []
    with connection(read_only=True) as conn:
        for name in targets:
            cols = conn.execute(
                """
                select column_name, data_type
                from information_schema.columns
                where table_schema = ? and table_name = ?
                order by ordinal_position
                """,
                [cat.GOLD_SCHEMA, name],
            ).fetchall()
            if not cols:
                # Allowlisted but not materialized — surface it honestly.
                tables_info.append(
                    {"name": name, "qualified": cat.qualified(name), "materialized": False, "columns": []}
                )
                continue
            count = conn.execute(f"select count(*) from {cat.qualified(name)}").fetchone()[0]
            tables_info.append(
                {
                    "name": name,
                    "qualified": cat.qualified(name),
                    "materialized": True,
                    "row_count": int(count),
                    "columns": [{"name": c, "type": t} for c, t in cols],
                }
            )

    intents = [
        {
            "name": it.name,
            "title": it.title,
            "description": it.description,
            "params": [p.name for p in it.params] + ["limit", "order_by"],
            "order_by": list(it.order_by_whitelist),
        }
        for it in cat.INTENTS.values()
    ]
    reports = [
        {"name": r.name, "title": r.title, "description": r.description,
         "sections": [s.title for s in r.sections]}
        for r in cat.REPORTS.values()
    ]
    return {
        "schema": cat.GOLD_SCHEMA,
        "tables": tables_info,
        "intents": intents,
        "reports": reports,
    }


# --- tool 2: execute_analytical_query -------------------------------------


def _bind_intent(intent: cat.Intent, params: dict[str, Any]) -> dict[str, Any]:
    """Build the DuckDB $param map for ``intent`` from caller ``params``.

    Unknown params are rejected (typo protection). Declared-but-absent params
    bind to NULL, which the SQL templates treat as "no filter".
    """
    declared = {p.name for p in intent.params}
    unknown = set(params) - declared - {"limit", "order_by"}
    if unknown:
        raise IntentError(
            f"unknown parameter(s) for intent {intent.name!r}: {', '.join(sorted(unknown))}; "
            f"allowed: {', '.join(sorted(declared)) or '(none)'}"
        )
    return {p.name: params.get(p.name, p.default) for p in intent.params}


def execute_analytical_query(
    intent: str,
    params: dict[str, Any] | None = None,
    limit: int | None = None,
    order_by: str | None = None,
) -> dict[str, Any]:
    """Run a curated intent against gold and return deterministic rows.

    Args:
        intent: a key in ``gold_catalog.INTENTS``.
        params: intent-specific filter values (bound, never interpolated).
        limit: max rows (clamped to ``MAX_LIMIT``).
        order_by: ``"col"`` or ``"col asc|desc"`` from the intent whitelist.

    Returns a dict with the resolved SQL (for transparency/auditing), the bound
    parameters, the column list, the row dicts, and the row count.
    """
    params = dict(params or {})
    if intent not in cat.INTENTS:
        raise IntentError(
            f"unknown intent {intent!r}; available: {', '.join(cat.INTENTS)}"
        )
    spec = cat.INTENTS[intent]

    bound = _bind_intent(spec, params)
    n = _clamp_limit(limit)
    order_clause = _resolve_order_by(spec, order_by)

    sql = spec.sql.strip()
    if order_clause:
        sql = f"{sql}\norder by {order_clause}"
    sql = f"{sql}\nlimit {n}"

    # Defense-in-depth: validate the fully-assembled body is a read-only SELECT
    # even though we authored it from a static template.
    safe_sql = assert_read_only_select(sql)

    with connection(read_only=True) as conn:
        cur = conn.execute(safe_sql, bound)
        rows = _rows_to_dicts(cur)

    return {
        "intent": intent,
        "sql": safe_sql,
        "params": {k: _jsonable(v) for k, v in bound.items()},
        "limit": n,
        "order_by": order_clause,
        "columns": list(rows[0].keys()) if rows else [],
        "row_count": len(rows),
        "rows": rows,
    }


# --- tool 3: generate_report ----------------------------------------------


def generate_report(
    report: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble a named multi-section report from curated intents.

    Each section is an :func:`execute_analytical_query` run; caller ``params``
    (e.g. a date window) are layered over each section's static params, so the
    whole report shares one window and stays deterministic.
    """
    params = dict(params or {})
    if report not in cat.REPORTS:
        raise IntentError(
            f"unknown report {report!r}; available: {', '.join(cat.REPORTS)}"
        )
    spec = cat.REPORTS[report]

    sections: list[dict[str, Any]] = []
    for section in spec.sections:
        # Only forward caller params the section's intent actually declares, so a
        # window meant for date-aware sections does not error on others.
        intent_spec = cat.INTENTS[section.intent]
        declared = {p.name for p in intent_spec.params}
        merged = dict(section.params)
        merged.update({k: v for k, v in params.items() if k in declared})
        result = execute_analytical_query(
            intent=section.intent,
            params=merged,
            limit=section.limit,
        )
        sections.append(
            {
                "key": section.key,
                "title": section.title,
                "intent": section.intent,
                "row_count": result["row_count"],
                "columns": result["columns"],
                "rows": result["rows"],
            }
        )

    return {
        "report": report,
        "title": spec.title,
        "description": spec.description,
        "params": params,
        "section_count": len(sections),
        "sections": sections,
    }
