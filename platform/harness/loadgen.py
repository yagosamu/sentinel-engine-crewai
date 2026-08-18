"""Concurrent load generators for the C4h harness.

Two worker families run simultaneously for the run window:

* :func:`oltp_writer` — the TRANSACTIONAL PATH. Inserts FK-valid orders in
  small batches at a steady rate that, summed across all writer workers, hits
  the 75k-orders/day-equivalent target. One commit per batch; per-commit latency
  is recorded. This is the path AC-1 protects.

* :func:`analytics_reader` — a representative ANALYTICAL read load against the
  source (aggregations / joins / scans of the kind an analytics consumer would
  run on Postgres). Read-only, tagged ``dagster_ingest``. AC-1 asserts this load
  induces ZERO lock-wait on the transactional path.

Both honour a shared ``stop`` event and a steady-rate token so the run is
duration-independent (smoke vs full differ only in duration/concurrency).
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from platform.harness.config import HarnessConfig
from platform.harness.pg_session import analytics_session, oltp_session

import psycopg
from faker import Faker

from src.gen import repository as repo
from src.seed.factories import EcommerceFactory


# Representative analytical queries against the SOURCE. Deliberately heavier than
# OLTP point-reads (group-bys, joins, range scans) so the read load is realistic.
# The orders->customers join column is drift-resolved at runtime (schema_drift
# may have renamed orders.customer_id -> user_id), mirroring how the OLTP writer
# and src.gen resolve the live column.
def _analytics_queries(customer_column: str) -> tuple[str, ...]:
    return (
        """
        SELECT date_trunc('day', ordered_at) AS d, count(*), sum(total_amount)
        FROM orders
        WHERE ordered_at >= now() - interval '30 days'
        GROUP BY 1 ORDER BY 1 DESC
        """,
        """
        SELECT p.category, count(*) AS n, sum(o.total_amount) AS rev
        FROM orders o JOIN products p ON p.product_id = o.product_id
        GROUP BY 1 ORDER BY rev DESC
        """,
        f"""
        SELECT c.segment, count(*) AS orders, avg(o.total_amount) AS aov
        FROM orders o JOIN customers c ON c.customer_id = o.{customer_column}
        GROUP BY 1 ORDER BY orders DESC
        """,
        """
        SELECT o.status, count(*), sum(o.total_amount)
        FROM orders o
        GROUP BY 1 ORDER BY 2 DESC
        """,
    )


@dataclass(slots=True)
class WorkerStats:
    """Raw per-worker tallies (operations, errors, timeouts, latencies).

    One per worker; ``merge_stats`` combines a role's workers before summarising.
    """

    role: str
    operations: int = 0
    errors: int = 0
    timeouts: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    rows_committed: int = 0


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def _build_order_rows(
    factory: EcommerceFactory,
    customer_ids: list[int],
    products: list[tuple],
    batch_size: int,
) -> list[tuple]:
    rows: list[tuple] = []
    not_before = datetime.now(UTC).replace(year=2020)
    for _ in range(batch_size):
        customer_id = random.choice(customer_ids)
        product = random.choice(products)
        order = factory.order(customer_id, product, not_before=not_before)
        rows.append(
            (
                customer_id,
                order.product_id,
                order.quantity,
                order.unit_price,
                order.total_amount,
                order.status,
                datetime.now(UTC),
            )
        )
    return rows


def oltp_writer(
    config: HarnessConfig,
    worker_index: int,
    stop: threading.Event,
    stats: WorkerStats,
) -> None:
    """Steady-rate transactional INSERT loop. One commit per batch."""
    try:
        conn = oltp_session(config.oltp_statement_timeout_ms)
    except psycopg.Error:
        # A worker that cannot even connect must not crash the run; record it as
        # a setup error so the verdict still renders (likely INCONCLUSIVE).
        stats.errors += 1
        return
    factory = EcommerceFactory(Faker())
    try:
        customer_column = repo.order_customer_column(conn)
        customer_ids = repo.sample_customer_ids(conn, 200)
        products = repo.sample_products(conn, 200)
        if not customer_ids or not products:
            return
        columns = [customer_column, "product_id", "quantity", "unit_price", "total_amount", "status", "ordered_at"]
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO orders ({', '.join(columns)}) VALUES ({placeholders})"

        # In throttled mode, pace each worker so all workers together emit
        # min_orders_per_day. In peak mode (the gate default) there is no pacing:
        # writers commit as fast as Postgres lets them, applying real peak
        # pressure so the isolation claim is tested under saturation.
        batch_interval = 0.0
        if not config.peak_mode:
            batch_interval = (config.oltp_batch_size * config.oltp_workers) / config.throttled_orders_per_second

        next_at = time.monotonic()
        while not stop.is_set():
            rows = _build_order_rows(factory, customer_ids, products, config.oltp_batch_size)
            t0 = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, rows)
                conn.commit()
                stats.latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                stats.operations += 1
                stats.rows_committed += len(rows)
            except psycopg.errors.QueryCanceled:
                conn.rollback()
                stats.timeouts += 1
            except psycopg.Error:
                conn.rollback()
                stats.errors += 1

            if batch_interval:
                # Throttled: pace to the steady rate; if behind, fire now.
                next_at += batch_interval
                sleep_for = next_at - time.monotonic()
                if sleep_for > 0:
                    stop.wait(sleep_for)
                else:
                    next_at = time.monotonic()
    finally:
        conn.close()


def analytics_reader(
    config: HarnessConfig,
    worker_index: int,
    stop: threading.Event,
    stats: WorkerStats,
) -> None:
    """Continuous representative analytical read loop (read-only)."""
    try:
        conn = analytics_session(config.analytics_statement_timeout_ms)
    except psycopg.Error:
        stats.errors += 1
        return
    try:
        customer_column = repo.order_customer_column(conn)
        queries = _analytics_queries(customer_column)
        i = worker_index
        while not stop.is_set():
            query = queries[i % len(queries)]
            i += 1
            t0 = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    cur.fetchall()
                conn.commit()
                stats.latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                stats.operations += 1
            except psycopg.errors.QueryCanceled:
                conn.rollback()
                stats.timeouts += 1
            except psycopg.Error:
                conn.rollback()
                stats.errors += 1
    finally:
        conn.close()


@dataclass(slots=True)
class LatencySummary:
    """Aggregated latency for one role (count, errors, timeouts, p50/p95/p99/max).

    The reportable view of merged :class:`WorkerStats`; consumed by the verdict.
    """

    role: str
    count: int
    errors: int
    timeouts: int
    rows_committed: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float

    @classmethod
    def from_stats(cls, stats: WorkerStats) -> LatencySummary:
        ordered = sorted(stats.latencies_ms)
        return cls(
            role=stats.role,
            count=stats.operations,
            errors=stats.errors,
            timeouts=stats.timeouts,
            rows_committed=stats.rows_committed,
            p50_ms=round(_percentile(ordered, 0.50), 2),
            p95_ms=round(_percentile(ordered, 0.95), 2),
            p99_ms=round(_percentile(ordered, 0.99), 2),
            max_ms=round(ordered[-1], 2) if ordered else 0.0,
        )


def merge_stats(role: str, all_stats: list[WorkerStats]) -> WorkerStats:
    """Combine per-worker stats of one role into a single WorkerStats."""
    merged = WorkerStats(role=role)
    for s in all_stats:
        merged.operations += s.operations
        merged.errors += s.errors
        merged.timeouts += s.timeouts
        merged.rows_committed += s.rows_committed
        merged.latencies_ms.extend(s.latencies_ms)
    return merged
