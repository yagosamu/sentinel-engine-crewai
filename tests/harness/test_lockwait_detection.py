"""Integration tests for the C4h harness against a live Postgres.

The headline test is a TRUE-POSITIVE check: a gate that can only ever say PASS
is worthless, so we inject a real analytics-attributable lock-wait and assert
the monitor catches it. We also run the smoke profile end to end and assert a
clean PASS.

Both tests skip cleanly if Postgres is unreachable (so CI without a DB is green)
or unseeded.
"""

from __future__ import annotations

import contextlib
import threading
import time
from platform.harness.cli import main as cli_main
from platform.harness.config import smoke_profile
from platform.harness.monitor import LockWaitMonitor
from platform.harness.runner import run

import psycopg
import pytest

from src.db.connection import conninfo
from src.gen import repository as repo


def _postgres_ready() -> bool:
    try:
        conn = psycopg.connect(**conninfo(), connect_timeout=2)
    except psycopg.Error:
        return False
    try:
        if not repo.sample_customer_ids(conn, 1) or not repo.sample_products(conn, 1):
            return False
    finally:
        conn.close()
    return True


pytestmark = pytest.mark.skipif(not _postgres_ready(), reason="Postgres not reachable/seeded")


def _tagged(app: str) -> psycopg.Connection:
    info = conninfo()
    info["application_name"] = app
    return psycopg.connect(**info, autocommit=False)


def test_monitor_detects_analytics_attributable_lock_wait() -> None:
    """A 'dagster_ingest' session holding a table lock that blocks an
    'oltp_writer' MUST be flagged as analytics-attributable."""
    customer_column = repo.order_customer_column(_tagged("probe"))

    analytics = _tagged("dagster_ingest")
    with analytics.cursor() as cur:
        cur.execute("LOCK TABLE orders IN ACCESS EXCLUSIVE MODE")

    detected = 0
    try:
        with LockWaitMonitor(0.1) as mon:

            def writer() -> None:
                w = _tagged("oltp_writer")
                with w.cursor() as cur:
                    cur.execute("SET statement_timeout = 3000")
                w.commit()
                with contextlib.suppress(psycopg.Error):
                    with w.cursor() as cur:
                        cur.execute(
                            f"INSERT INTO orders ({customer_column}, product_id, quantity, "
                            "unit_price, total_amount, status, ordered_at) "
                            f"SELECT {customer_column}, product_id, 1, 1.0, 1.0, 'placed', now() "
                            "FROM orders LIMIT 1"
                        )
                    w.commit()
                w.close()

            t = threading.Thread(target=writer)
            t.start()
            time.sleep(1.2)  # let the monitor sample the contention
            analytics.rollback()  # release the lock so the writer unblocks
            t.join(timeout=5.0)
            time.sleep(0.3)
        detected = len(mon.result.analytics_attributable)
    finally:
        with contextlib.suppress(psycopg.Error):
            analytics.close()

    assert detected > 0, "monitor failed to detect an injected analytics-attributable lock-wait"
    event = mon.result.analytics_attributable[0]
    assert event.waiter_app == "oltp_writer"
    assert event.blocker_app == "dagster_ingest"


def test_smoke_profile_passes_under_clean_concurrency() -> None:
    """End-to-end smoke: peak OLTP + concurrent analytics should isolate cleanly."""
    report = run(smoke_profile())
    assert report.load_floor_met is True, report.verdict
    assert report.analytics_attributable_lock_waits == 0, report.verdict
    assert report.passed is True, report.verdict
    assert report.oltp["count"] > 0
    assert report.analytics["count"] > 0
    assert report.samples_taken > 0


def test_cli_smoke_returns_zero() -> None:
    """The CLI gate exits 0 on a clean smoke run."""
    assert cli_main(["--profile", "smoke"]) == 0
