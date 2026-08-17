"""The 14-failure chaos registry -- the source of truth for what can break.

Each failure is a :class:`Failure` subclass declaring four metadata strings
(``key``, ``summary``, ``detected_by``, ``unlocks``) and an ``inject(conn)``
method that mutates source Postgres and returns an :class:`InjectionResult`.

The registry splits two ways:

* **Base-crew failures** (``unlocks`` starts with ``"base crew"``) -- the
  detect / diagnose / report core the four-capability crew handles directly.
* **Feature-unlocking failures** -- each forces one specific CrewAI capability
  (Memory, Knowledge/RAG, Human-in-the-loop, Guardrails, tool reliability,
  Flows). The ``unlocks`` strings here are the de-facto failure -> capability
  map that rule R6 binds the crew to; until the KB reference is populated,
  these fields are the authoritative version of that map.

Chaos is not self-reversing. Several injectors ``DROP CONSTRAINT`` or mutate
existing rows with no restore path (only ``reset-schema`` reverts the
``schema_drift`` column rename). A reproducible inject -> detect -> score run
must rebuild a clean baseline first (rule R7).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg

from src.gen import repository as repo


def _order_columns(conn: psycopg.Connection) -> list[str]:
    """Build the insert column list for ``orders``, drift-aware in slot 0."""
    return [
        repo.order_customer_column(conn),
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "status",
        "ordered_at",
    ]


@dataclass(frozen=True, slots=True)
class InjectionResult:
    """What an injection did, as the engine records it to the ledger.

    ``failure`` is the registry key, ``detail`` is a human-readable note (e.g.
    the affected ``order_id``), and ``detected_by`` names the crew role
    expected to surface it. The engine passes these three fields straight to
    :func:`repository.record_incident`.
    """

    failure: str
    detail: str
    detected_by: str


class Failure:
    """Base class for the 14 injectable failures.

    Subclasses set the four metadata class attributes and override
    :meth:`inject`. ``key`` is the registry/CLI name; ``summary`` is the
    one-line CLI description; ``detected_by`` is the responsible crew role;
    ``unlocks`` documents which CrewAI capability the failure forces (R6).
    """

    key: str = ""
    summary: str = ""
    detected_by: str = ""
    unlocks: str = ""

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        """Mutate the source DB and return what was done. Must not commit.

        Committing is the engine's job; the injector only stages the change so
        the failure and its ledger row land in one transaction.
        """
        raise NotImplementedError


def _disable_order_checks(conn: psycopg.Connection) -> None:
    """Drop the ``orders`` CHECK/NOT-NULL guards so bad rows can land.

    DESTRUCTIVE and not self-reversing: the dropped constraints are not
    restored by the generator, so a clean baseline requires a full schema
    rebuild (``make reset`` + ``make seed``), not ``reset-schema`` (R7).
    """
    customer_column = repo.order_customer_column(conn)
    repo.execute(conn, "ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_unit_price_check")
    repo.execute(conn, "ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_quantity_check")
    repo.execute(conn, "ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_total_amount_check")
    repo.execute(conn, f"ALTER TABLE orders ALTER COLUMN {customer_column} DROP NOT NULL")


class NegativePrice(Failure):
    """Insert an order priced below zero. Drops the price CHECK first."""

    key = "negative_price"
    summary = "Insert an order with a negative unit price and total."
    detected_by = "Data Profiler"
    unlocks = "base crew: Data Profiler tool (QueryDuckDB / ProfileTable)"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        _disable_order_checks(conn)
        customer_id = repo.sample_customer_ids(conn, 1)[0]
        product_id, _ = repo.sample_products(conn, 1)[0]
        price = Decimal("-49.99")
        order_id = repo.insert_order(
            conn,
            _order_columns(conn),
            (customer_id, product_id, 1, price, price, "placed", datetime.now(UTC)),
        )
        return InjectionResult(self.key, f"order_id={order_id} unit_price={price}", self.detected_by)


class MissingCustomer(Failure):
    """Insert an orphan order with a NULL customer ref. Drops NOT NULL first."""

    key = "missing_customer"
    summary = "Insert an order with a NULL customer_id (orphaned order)."
    detected_by = "Data Profiler"
    unlocks = "base crew: null/constraint profiling"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        _disable_order_checks(conn)
        customer_column = repo.order_customer_column(conn)
        product_id, unit_price = repo.sample_products(conn, 1)[0]
        repo.execute(
            conn,
            f"INSERT INTO orders ({customer_column}, product_id, quantity, unit_price, total_amount, status, "
            "ordered_at) VALUES (NULL, %s, %s, %s, %s, %s, %s)",
            (product_id, 1, unit_price, unit_price, "placed", datetime.now(UTC)),
        )
        return InjectionResult(self.key, f"inserted order with {customer_column}=NULL", self.detected_by)


class InvalidQuantity(Failure):
    """Insert an order with quantity ``-5``. Drops the quantity CHECK first."""

    key = "invalid_quantity"
    summary = "Insert an order with a non-positive quantity."
    detected_by = "Data Profiler"
    unlocks = "base crew: range/domain profiling"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        _disable_order_checks(conn)
        customer_id = repo.sample_customer_ids(conn, 1)[0]
        product_id, unit_price = repo.sample_products(conn, 1)[0]
        order_id = repo.insert_order(
            conn,
            _order_columns(conn),
            (customer_id, product_id, -5, unit_price, Decimal("0.00"), "placed", datetime.now(UTC)),
        )
        return InjectionResult(self.key, f"order_id={order_id} quantity=-5", self.detected_by)


class DuplicateOrder(Failure):
    """Clone the latest order row verbatim. No-op if the table is empty."""

    key = "duplicate_order"
    summary = "Re-insert the most recent order as an exact duplicate row."
    detected_by = "Data Profiler"
    unlocks = "base crew: uniqueness profiling"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        latest = repo.latest_order(conn)
        if latest is None:
            return InjectionResult(self.key, "no orders to duplicate", self.detected_by)
        _, customer_id, product_id, quantity, unit_price, total_amount, status, ordered_at = latest
        order_id = repo.insert_order(
            conn,
            _order_columns(conn),
            (customer_id, product_id, quantity, unit_price, total_amount, status, ordered_at),
        )
        return InjectionResult(self.key, f"duplicated into order_id={order_id}", self.detected_by)


class LateArrival(Failure):
    """Insert a valid order backdated 45 days to test freshness windows."""

    key = "late_arrival"
    summary = "Insert an order backdated 45 days (late-arriving data)."
    detected_by = "Data Profiler"
    unlocks = "base crew: freshness profiling"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        customer_id = repo.sample_customer_ids(conn, 1)[0]
        product_id, unit_price = repo.sample_products(conn, 1)[0]
        backdated = datetime.now(UTC) - timedelta(days=45)
        order_id = repo.insert_order(
            conn,
            _order_columns(conn),
            (customer_id, product_id, 1, unit_price, unit_price, "delivered", backdated),
        )
        return InjectionResult(self.key, f"order_id={order_id} ordered_at={backdated.date()}", self.detected_by)


class VolumeSpike(Failure):
    """Insert ``burst`` valid orders in one ``executemany`` (default 500)."""

    key = "volume_spike"
    summary = "Insert a sudden burst of orders (volume anomaly)."
    detected_by = "Data Profiler"
    unlocks = "base crew: volume/statistical profiling"

    def __init__(self, burst: int = 500) -> None:
        self.burst = burst

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        columns = _order_columns(conn)
        customers = repo.sample_customer_ids(conn, 50) or [None]
        products = repo.sample_products(conn, 50)
        now = datetime.now(UTC)
        rows = []
        for index in range(self.burst):
            customer_id = customers[index % len(customers)]
            product_id, unit_price = products[index % len(products)]
            rows.append((customer_id, product_id, 1, unit_price, unit_price, "placed", now))
        placeholders = ", ".join(["%s"] * len(columns))
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO orders ({', '.join(columns)}) VALUES ({placeholders})",
                rows,
            )
        return InjectionResult(self.key, f"inserted {self.burst} orders in one burst", self.detected_by)


class SchemaDrift(Failure):
    """Rename ``orders.customer_id`` to ``user_id``. Idempotent if already drifted.

    This is the one failure ``reset-schema`` can revert. Because the rename is
    live, every other query resolves the column via
    :func:`repository.order_customer_column`.
    """

    key = "schema_drift"
    summary = "Rename orders.customer_id -> user_id (breaks downstream models)."
    detected_by = "Log Analyst"
    unlocks = "base crew: Log Analyst tool (ReadDagsterLogs / ReadDbtRunResults)"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        current = repo.order_customer_column(conn)
        if current == "user_id":
            return InjectionResult(self.key, "already drifted (column is user_id)", self.detected_by)
        repo.execute(conn, "ALTER TABLE orders RENAME COLUMN customer_id TO user_id")
        return InjectionResult(self.key, "orders.customer_id renamed to user_id", self.detected_by)


class OrphanPayment(Failure):
    """Insert a payment for a non-existent order. Drops the payments FK first.

    The dropped foreign key is not restored (R7): a clean baseline needs a
    full schema rebuild.
    """

    key = "orphan_payment"
    summary = "Insert a payment referencing a non-existent order_id."
    detected_by = "Data Profiler"
    unlocks = "base crew: cross-table referential profiling"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        repo.execute(conn, "ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_order_id_fkey")
        repo.execute(
            conn,
            "INSERT INTO payments (order_id, method, amount, status, paid_at) VALUES (%s, %s, %s, %s, %s)",
            (999999999, "credit_card", Decimal("10.00"), "captured", datetime.now(UTC)),
        )
        return InjectionResult(self.key, "payment inserted for order_id=999999999", self.detected_by)


class RecurringIncident(Failure):
    """Re-inject a negative-price order and report its occurrence number.

    Counts prior ledger rows for this key so each injection's ``detail`` reads
    ``occurrence #N`` -- the signal a memory-equipped crew uses to recognise a
    repeat offender. Drops the price CHECK first.
    """

    key = "recurring_incident"
    summary = "Re-inject negative prices repeatedly so the same incident appears many times."
    detected_by = "Data Profiler"
    unlocks = "CrewAI Memory: recognise a repeat offender ('seen 3x this week') instead of cold-starting"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        _disable_order_checks(conn)
        columns = _order_columns(conn)
        customer_id = repo.sample_customer_ids(conn, 1)[0]
        product_id, _ = repo.sample_products(conn, 1)[0]
        price = Decimal("-19.99")
        order_id = repo.insert_order(
            conn,
            columns,
            (customer_id, product_id, 1, price, price, "placed", datetime.now(UTC)),
        )
        seen = repo.count_incidents(conn, self.key) + 1
        return InjectionResult(self.key, f"order_id={order_id} (occurrence #{seen})", self.detected_by)


class AmbiguousAnomaly(Failure):
    """Drop revenue two ways at once so the root cause is genuinely ambiguous.

    DESTRUCTIVE in place: cancels 200 existing orders and halves the price of
    20 random products. Both could explain a revenue dip; neither row is
    restored, so a clean baseline needs a reseed (R7).
    """

    key = "ambiguous_anomaly"
    summary = "Revenue drops via cancellations AND a price cut at once (two plausible root causes)."
    detected_by = "Data Profiler"
    unlocks = "CrewAI Knowledge/RAG: consult a runbook to disambiguate competing root causes"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        repo.execute(
            conn,
            "UPDATE orders SET status = 'cancelled' WHERE order_id IN "
            "(SELECT order_id FROM orders WHERE status <> 'cancelled' ORDER BY order_id DESC LIMIT 200)",
        )
        repo.execute(
            conn,
            "UPDATE products SET unit_price = round(unit_price * 0.5, 2) WHERE product_id IN "
            "(SELECT product_id FROM products ORDER BY random() LIMIT 20)",
        )
        return InjectionResult(self.key, "200 orders cancelled + 20 products price-cut 50%", self.detected_by)


class DestructiveFix(Failure):
    """Zero ``total_amount`` on 300 existing orders so the fix must be bulk.

    DESTRUCTIVE in place and not restored (R7): the only remediation is a bulk
    overwrite, which is what the human-in-the-loop approval gate is meant to
    guard.
    """

    key = "destructive_fix"
    summary = "Corrupt total_amount on many rows so the only fix is a bulk overwrite."
    detected_by = "Data Profiler"
    unlocks = "CrewAI Human-in-the-loop: a destructive remediation must pause for approval"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        repo.execute(
            conn,
            "UPDATE orders SET total_amount = 0 WHERE order_id IN "
            "(SELECT order_id FROM orders ORDER BY order_id DESC LIMIT 300)",
        )
        return InjectionResult(self.key, "zeroed total_amount on 300 orders (bulk fix required)", self.detected_by)


class MalformedData(Failure):
    """Overwrite ``status`` on 25 existing orders with control-char garbage.

    DESTRUCTIVE in place and not restored (R7). The unprintable payload is the
    noise a guardrail-validated, ``output_pydantic`` post-mortem must reject or
    normalise.
    """

    key = "malformed_data"
    summary = "Inject garbage into status fields (free-text noise the agent must summarise)."
    detected_by = "Data Profiler"
    unlocks = "CrewAI Guardrails + output_pydantic: force a typed, validated post-mortem"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        garbage = "���/NULL/<script>/0x00 ¿status?"
        repo.execute(
            conn,
            "UPDATE orders SET status = %s WHERE order_id IN "
            "(SELECT order_id FROM orders ORDER BY order_id DESC LIMIT 25)",
            (garbage,),
        )
        return InjectionResult(self.key, "wrote garbage status to 25 orders", self.detected_by)


class SlowSource(Failure):
    """Stall the source with ``pg_sleep`` to simulate an unresponsive DB.

    Sets ``lock_timeout = 1s`` then sleeps ``seconds`` (default 8). Leaves no
    row mutation behind -- it only makes a tool slow, exercising retry and
    timeout handling.
    """

    key = "slow_source"
    summary = "Hold a lock on orders to make the source slow/unresponsive for a while."
    detected_by = "Log Analyst"
    unlocks = "CrewAI tool reliability: max_retry, timeouts and fallbacks on flaky tools"

    def __init__(self, seconds: int = 8) -> None:
        self.seconds = seconds

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '1s'")
            cur.execute(f"SELECT pg_sleep({self.seconds})")
        return InjectionResult(self.key, f"source stalled for {self.seconds}s", self.detected_by)


class MultiFailureCascade(Failure):
    """Fire three sub-failures at once, recording each one individually.

    Composes ``missing_customer`` + ``volume_spike`` + ``schema_drift`` and
    calls :func:`repository.record_incident` for each sub-result here. The
    engine then records the cascade itself, so **one cascade injection writes
    four** ``injected_incidents`` rows (three sub-incidents + the cascade). Any
    I4 scoring consumer must account for that fan-out when matching diagnoses
    to ground truth.
    """

    key = "multi_failure_cascade"
    summary = "Fire schema drift + nulls + a volume spike together (mixed incident)."
    detected_by = "Manager"
    unlocks = "CrewAI Flows + conditional routing: send each failure type to the right squad"

    def inject(self, conn: psycopg.Connection) -> InjectionResult:
        parts: list[str] = []
        for key in ("missing_customer", "volume_spike", "schema_drift"):
            result = REGISTRY[key].inject(conn)
            repo.record_incident(conn, result.failure, result.detail, result.detected_by)
            parts.append(result.failure)
        return InjectionResult(self.key, "cascade: " + ", ".join(parts), self.detected_by)


REGISTRY: dict[str, Failure] = {
    failure.key: failure
    for failure in (
        NegativePrice(),
        MissingCustomer(),
        InvalidQuantity(),
        DuplicateOrder(),
        LateArrival(),
        VolumeSpike(),
        SchemaDrift(),
        OrphanPayment(),
        RecurringIncident(),
        AmbiguousAnomaly(),
        DestructiveFix(),
        MalformedData(),
        SlowSource(),
        MultiFailureCascade(),
    )
}


def get(key: str) -> Failure:
    """Look up a failure by key, raising ``KeyError`` (listing valid keys)."""
    if key not in REGISTRY:
        available = ", ".join(sorted(REGISTRY))
        raise KeyError(f"unknown failure '{key}'. available: {available}")
    return REGISTRY[key]
