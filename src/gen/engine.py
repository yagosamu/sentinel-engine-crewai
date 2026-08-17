"""Traffic emission and failure orchestration for the chaos generator.

Normal traffic and injections share one transaction discipline: the engine is
the layer that commits, so a failure and its ground-truth ledger row land
together (and an injector's mutation never leaks out without its incident
record).
"""

from __future__ import annotations

import random
import time
from datetime import UTC, datetime

import psycopg
from faker import Faker

from src.gen import repository as repo
from src.gen.failures import REGISTRY, InjectionResult
from src.seed.factories import EcommerceFactory


class TrafficGenerator:
    """Emits batches of plausible normal orders into source Postgres."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        self.factory = EcommerceFactory(Faker())

    def emit(self, count: int) -> int:
        """Insert ``count`` normal orders in one batch; return rows inserted.

        Samples existing customers/products, builds each order through the
        seed factory (so values stay realistic), resolves the customer column
        drift-aware, then bulk-inserts and commits. Returns ``0`` without
        writing if there are no customers or products to reference.
        """
        customer_column = repo.order_customer_column(self.conn)
        customers = repo.sample_customer_ids(self.conn, min(count, 200))
        products = repo.sample_products(self.conn, min(count, 200))
        if not customers or not products:
            return 0

        columns = [customer_column, "product_id", "quantity", "unit_price", "total_amount", "status", "ordered_at"]
        rows = []
        for _ in range(count):
            customer_id = random.choice(customers)
            product = random.choice(products)
            order = self.factory.order(customer_id, product, not_before=datetime.now(UTC).replace(year=2020))
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

        placeholders = ", ".join(["%s"] * len(columns))
        with self.conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO orders ({', '.join(columns)}) VALUES ({placeholders})",
                rows,
            )
        self.conn.commit()
        return count


def run_traffic(conn: psycopg.Connection, count: int) -> int:
    """One-shot convenience wrapper around :meth:`TrafficGenerator.emit`."""
    inserted = TrafficGenerator(conn).emit(count)
    return inserted


def inject(conn: psycopg.Connection, key: str) -> InjectionResult:
    """Run failure ``key``, record it to the I4 ledger, and commit atomically.

    The injector's mutation and its ``injected_incidents`` row commit in the
    same transaction so ground truth can never diverge from what was injected
    (note: ``multi_failure_cascade`` records its own sub-incidents first, so
    that one key yields four ledger rows). Raises ``KeyError`` for an unknown
    ``key``.
    """
    from src.gen.failures import get

    result = get(key).inject(conn)
    repo.record_incident(conn, result.failure, result.detail, result.detected_by)
    conn.commit()
    return result


def watch(
    conn: psycopg.Connection,
    interval: float,
    batch: int,
    failure_every: int,
    failures: list[str],
    on_event,
) -> None:
    """Run the continuous traffic+chaos loop until interrupted.

    Each tick emits a ``batch`` of normal orders; every ``failure_every`` ticks
    it injects one random failure drawn from ``failures`` (or the whole
    registry when empty). ``on_event`` is called with a human-readable line per
    tick and per injection. ``failure_every`` of 0 disables injection. Loops
    forever, sleeping ``interval`` seconds between ticks; the CLI stops it on
    ``KeyboardInterrupt``.
    """
    generator = TrafficGenerator(conn)
    pool = failures or list(REGISTRY)
    tick = 0
    while True:
        tick += 1
        generator.emit(batch)
        conn.commit()
        on_event(f"tick {tick}: +{batch} orders")
        if failure_every and tick % failure_every == 0:
            key = random.choice(pool)
            result = inject(conn, key)
            on_event(f"tick {tick}: INJECTED {result.failure} ({result.detail})")
        time.sleep(interval)
