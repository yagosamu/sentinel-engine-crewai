"""Tagged Postgres sessions for the C4h harness.

Every connection the harness opens carries an ``application_name`` so that the
monitor can attribute lock-waits in ``pg_stat_activity`` (the AC-1 linchpin).
This module is the ONLY place the harness opens a Postgres connection, so the
attribution tags and per-role session settings live in one spot.

It reuses ``src.db.connection.conninfo()`` for host/port/credentials (the
harness's load driver legitimately touches the source DB — that exception is
called out in the build invariants) but layers the harness-specific
``application_name`` and session GUCs on top.
"""

from __future__ import annotations

from platform.harness.config import ANALYTICS_APP_NAME, OLTP_APP_NAME

import psycopg
from psycopg import sql

from src.db.connection import conninfo


def _connect(app_name: str, *, read_only: bool, statement_timeout_ms: int) -> psycopg.Connection:
    info = conninfo()
    info["application_name"] = app_name
    conn = psycopg.connect(**info, autocommit=False)
    with conn.cursor() as cur:
        # SET does not accept server-side bind parameters, so compose the value
        # as a literal. It is coerced to int first, so it is injection-safe.
        # statement_timeout keeps a stuck query from outliving the test window,
        # which is what keeps the AC-1 verdict interpretable.
        cur.execute(
            sql.SQL("SET statement_timeout = {}").format(sql.Literal(int(statement_timeout_ms)))
        )
        if read_only:
            cur.execute("SET default_transaction_read_only = on")
    conn.commit()
    return conn


def oltp_session(statement_timeout_ms: int) -> psycopg.Connection:
    """A transactional WRITER session — the path AC-1 protects.

    Tagged ``oltp_writer``. Read/write, short statement_timeout.
    """
    return _connect(OLTP_APP_NAME, read_only=False, statement_timeout_ms=statement_timeout_ms)


def analytics_session(statement_timeout_ms: int) -> psycopg.Connection:
    """A read-only ANALYTICS session, tagged ``dagster_ingest``.

    Read-only at the transaction level so it can never *itself* take a write
    lock; AC-1 then measures whether even this read load induces any lock-wait
    on the transactional path (it must not).
    """
    return _connect(ANALYTICS_APP_NAME, read_only=True, statement_timeout_ms=statement_timeout_ms)


def monitor_session() -> psycopg.Connection:
    """A read-only observer session for the lock-wait sampler.

    Tagged distinctly (``c4h_monitor``) and run in autocommit so each sample is
    a fresh snapshot of pg_locks / pg_stat_activity with no held transaction.
    """
    info = conninfo()
    info["application_name"] = "c4h_monitor"
    conn = psycopg.connect(**info, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    return conn
