"""Parameterised SQL the chaos generator runs against source Postgres.

Every query here uses bound placeholders (rule R8). The one piece of dynamic
identifier resolution -- ``order_customer_column`` -- reads
``information_schema`` rather than user input so it stays correct after the
``schema_drift`` failure renames the column.
"""

from __future__ import annotations

from decimal import Decimal

import psycopg

from src.db.connection import connect


def sample_customer_ids(conn: psycopg.Connection, limit: int) -> list[int]:
    """Return up to ``limit`` random ``customer_id`` values."""
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT %s", (limit,))
        return [row[0] for row in cur.fetchall()]


def sample_products(conn: psycopg.Connection, limit: int) -> list[tuple[int, Decimal]]:
    """Return up to ``limit`` random ``(product_id, unit_price)`` pairs."""
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, unit_price FROM products ORDER BY random() LIMIT %s", (limit,))
        return [(row[0], row[1]) for row in cur.fetchall()]


def latest_order(conn: psycopg.Connection) -> tuple | None:
    """Return the most recent order row, or ``None`` if the table is empty.

    The customer column is resolved at call time via ``order_customer_column``
    so this works whether or not ``schema_drift`` has renamed it; used by the
    ``duplicate_order`` injector to clone the latest row verbatim.
    """
    customer_column = order_customer_column(conn)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT order_id, {customer_column}, product_id, quantity, unit_price, total_amount, status, ordered_at "
            "FROM orders ORDER BY order_id DESC LIMIT 1"
        )
        return cur.fetchone()


def order_customer_column(conn: psycopg.Connection) -> str:
    """Resolve the live name of the ``orders`` customer-reference column.

    The ``schema_drift`` failure renames ``orders.customer_id`` to
    ``user_id``. Every generator query that touches that column calls this
    first so it keeps working after drift: it looks the name up in
    ``information_schema.columns`` and falls back to ``customer_id`` when the
    column is missing entirely. This indirection is why injectors and traffic
    build their column lists dynamically instead of hardcoding the name.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'orders' AND column_name IN ('customer_id', 'user_id')"
        )
        row = cur.fetchone()
    return row[0] if row else "customer_id"


def execute(conn: psycopg.Connection, statement: str, params: tuple = ()) -> None:
    """Run a single parameterised statement, discarding any result set."""
    with conn.cursor() as cur:
        cur.execute(statement, params)


def insert_order(conn: psycopg.Connection, columns: list[str], values: tuple) -> int:
    """Insert one orders row from a dynamic column list, returning its PK.

    ``columns`` is built by the caller from ``order_customer_column`` so the
    first column tracks any schema drift.
    """
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO orders ({cols}) VALUES ({placeholders}) RETURNING order_id", values)
        return cur.fetchone()[0]


def record_incident(conn: psycopg.Connection, failure_key: str, detail: str, detected_by: str) -> None:
    """Append one row to the ``injected_incidents`` ground-truth ledger.

    This is the write side of interface I4: every injection records what was
    done so the future Sentinel can score its diagnosis against ground truth.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO injected_incidents (failure_key, detail, detected_by) VALUES (%s, %s, %s)",
            (failure_key, detail, detected_by),
        )


def count_incidents(conn: psycopg.Connection, failure_key: str) -> int:
    """Count ledger rows already recorded for ``failure_key``.

    Used by ``recurring_incident`` to report which occurrence it is.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM injected_incidents WHERE failure_key = %s", (failure_key,))
        return cur.fetchone()[0]


def session() -> psycopg.Connection:
    """Open a source connection for a CLI command (alias for ``connect``)."""
    return connect()
