"""Source Postgres connection factory and bulk-insert helpers.

The single place the source layer opens a connection and the single place
primary keys are returned in insert order so the seeder can thread foreign
keys across tables. No secrets live here: connection settings come from
``POSTGRES_*`` environment variables (see ``conninfo``).
"""

from __future__ import annotations

import os
from collections.abc import Sequence

import psycopg


def conninfo() -> dict[str, object]:
    """Build psycopg connection kwargs from ``POSTGRES_*`` env vars.

    Defaults target the local docker-compose Postgres (``localhost:5432``,
    db ``ecommerce``, user/password ``postgres``). Reading from the
    environment keeps credentials out of source (rule R8).
    """
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "ecommerce"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    }


def connect() -> psycopg.Connection:
    """Open a source Postgres connection with ``autocommit=False``.

    Callers own the transaction boundary and must ``commit()`` explicitly;
    nothing here commits on the caller's behalf.
    """
    return psycopg.connect(**conninfo(), autocommit=False)


def insert_returning_ids(
    conn: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple],
) -> list[int]:
    """Bulk-insert ``rows`` into ``table`` and return the new PKs in order.

    The primary-key column name is derived by de-pluralising the table name:
    ``customers`` -> ``customer_id``. This convention is what lets the seeder
    feed one table's returned IDs straight into the next table's foreign keys.
    It silently produces the wrong column for any table whose name is not the
    plural of its ``<singular>_id`` PK.

    Uses ``executemany(..., returning=True)`` and walks the result sets with
    ``nextset()`` so the returned list aligns 1:1 with ``rows`` in insert
    order. Returns ``[]`` for an empty input without touching the database.
    """
    if not rows:
        return []
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    pk = f"{table[:-1]}_id"
    statement = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) RETURNING {pk}"
    ids: list[int] = []
    with conn.cursor() as cur:
        cur.executemany(statement, rows, returning=True)
        while True:
            ids.append(cur.fetchone()[0])
            if not cur.nextset():
                break
    return ids


def count(conn: psycopg.Connection, table: str) -> int:
    """Return ``SELECT count(*)`` for ``table``."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def truncate_all(conn: psycopg.Connection) -> None:
    """Truncate the four business tables, resetting IDENTITY sequences.

    Deliberately excludes ``injected_incidents``. That table is the I4
    ground-truth oracle the future Sentinel scores against; preserving it
    across a reseed is load-bearing for the reset-to-clean discipline in
    rule R7, so a reseed must never wipe the incident ledger.
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE payments, orders, products, customers RESTART IDENTITY CASCADE")
