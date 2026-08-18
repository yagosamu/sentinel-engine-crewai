"""Dagster resources for C2 ingestion.

Two resources:

- :class:`PostgresResource` — the READ-ONLY source connection. Every session is
  forced ``default_transaction_read_only=on`` and tagged
  ``application_name='dagster_ingest'`` (the AC-1 attribution linchpin: C4h proves
  zero analytics-attributable lock-wait by filtering on this exact tag). A
  ``statement_timeout`` keeps a stalled source (``slow_source`` failure) from
  hanging the run; it MUST stay below the C4h peak-load window so AC-1 remains
  interpretable.

- :class:`DuckDBResource` — wraps the C4 warehouse substrate
  (:mod:`platform.warehouse.connection`). C2 is the SOLE writer of the ``raw``
  schema. This resource does NOT create ``paths.py`` / ``connection.py`` — it
  consumes them, per the ownership map (C4 owns the warehouse substrate).
"""

from __future__ import annotations

from contextlib import contextmanager
from platform.warehouse.connection import connect as _duckdb_connect
from typing import TYPE_CHECKING

import psycopg
from dagster import ConfigurableResource

from src.db.connection import conninfo

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb


# The session tag C4h filters on to attribute (or exonerate) lock-wait to the
# ingest workload. Changing this string silently breaks the AC-1 measurement.
INGEST_APPLICATION_NAME = "dagster_ingest"


class PostgresResource(ConfigurableResource):
    """Read-only Postgres source connection for incremental extraction.

    Config mirrors :func:`src.db.connection.conninfo` defaults via env vars, so
    nothing has to be passed explicitly in ``Definitions``; the resource reads
    ``POSTGRES_HOST/PORT/DB/USER/PASSWORD`` at connect time.
    """

    # statement_timeout in milliseconds. Default 30s: long enough for a 75k/day
    # window scan, short enough that slow_source's 8s lock surfaces as a bounded
    # failure rather than an unbounded hang. Keep below the C4h peak window.
    statement_timeout_ms: int = 30_000

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        """Yield a read-only, tagged psycopg connection (auto-closed).

        The connection is opened ``autocommit=True`` because extraction is
        read-only — no transaction state to manage, and it avoids holding a
        snapshot open across the whole window scan.
        """
        info = conninfo()
        conn = psycopg.connect(
            **info,
            autocommit=True,
            application_name=INGEST_APPLICATION_NAME,
        )
        try:
            with conn.cursor() as cur:
                # Belt-and-suspenders: tag again (some poolers strip the kwarg),
                # force read-only at the session level, and bound statement time.
                # Postgres SET does not accept bind parameters, so these values
                # are interpolated — both are server-controlled constants (a fixed
                # app-name literal and an int we coerce), never user input.
                timeout_ms = int(self.statement_timeout_ms)
                cur.execute(f"SET application_name = '{INGEST_APPLICATION_NAME}'")
                cur.execute("SET default_transaction_read_only = on")
                cur.execute(f"SET statement_timeout = {timeout_ms}")
            yield conn
        finally:
            conn.close()


class DuckDBResource(ConfigurableResource):
    """Read/write handle to the shared warehouse for the SOLE raw writer (C2).

    Thin wrapper over :func:`platform.warehouse.connection.connect`. Opens
    read/write (writers leave ``read_only=False``); the orchestration invariant
    (ingest THEN dbt, serialized) guarantees no concurrent writer.
    """

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a warehouse connection (auto-closed)."""
        conn = _duckdb_connect(read_only=read_only)
        try:
            yield conn
        finally:
            conn.close()
