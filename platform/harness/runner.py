"""Orchestrates one AC-1 harness run.

Spins up N transactional writer threads + M analytics reader threads + one
lock-wait monitor thread, sustains the load for the configured window, then
tears everything down and builds the verdict.

Preconditions checked up front (fail fast with a clear message):
  * Postgres reachable, and
  * customers + products present (writers need FK-valid references).
"""

from __future__ import annotations

import threading
import time
from platform.harness.config import HarnessConfig
from platform.harness.loadgen import (
    LatencySummary,
    WorkerStats,
    analytics_reader,
    merge_stats,
    oltp_writer,
)
from platform.harness.monitor import LockWaitMonitor
from platform.harness.pg_session import monitor_session
from platform.harness.verdict import HarnessReport, build_report

import psycopg

from src.gen import repository as repo


class PreconditionError(RuntimeError):
    """Raised when the environment is not ready to run the harness."""


def _check_preconditions() -> None:
    try:
        conn = monitor_session()
    except psycopg.Error as exc:
        raise PreconditionError(f"cannot reach Postgres: {exc}") from exc
    try:
        customers = repo.sample_customer_ids(conn, 1)
        products = repo.sample_products(conn, 1)
    finally:
        conn.close()
    if not customers or not products:
        raise PreconditionError(
            "no customers/products in the source DB — seed it first "
            "(make seed). Writers need FK-valid references."
        )


def run(config: HarnessConfig) -> HarnessReport:
    """Execute one AC-1 harness run and return its report."""
    _check_preconditions()

    stop = threading.Event()
    oltp_stats = [WorkerStats(role="oltp") for _ in range(config.oltp_workers)]
    analytics_stats = [WorkerStats(role="analytics") for _ in range(config.analytics_workers)]
    threads: list[threading.Thread] = []

    for i in range(config.oltp_workers):
        t = threading.Thread(
            target=oltp_writer, args=(config, i, stop, oltp_stats[i]), name=f"c4h-oltp-{i}", daemon=True
        )
        threads.append(t)
    for i in range(config.analytics_workers):
        t = threading.Thread(
            target=analytics_reader,
            args=(config, i, stop, analytics_stats[i]),
            name=f"c4h-analytics-{i}",
            daemon=True,
        )
        threads.append(t)

    start = time.monotonic()
    with LockWaitMonitor(config.sample_interval_s) as monitor:
        for t in threads:
            t.start()
        # Sustain the load for the window.
        stop.wait(config.duration_s)
        stop.set()
        for t in threads:
            t.join(timeout=10.0)
    duration_s = time.monotonic() - start

    oltp_summary = LatencySummary.from_stats(merge_stats("oltp_writer", oltp_stats))
    analytics_summary = LatencySummary.from_stats(merge_stats("dagster_ingest", analytics_stats))

    return build_report(config, duration_s, oltp_summary, analytics_summary, monitor.result)
