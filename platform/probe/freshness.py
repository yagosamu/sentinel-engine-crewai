"""C8 active freshness probe — the AC-3 gate (source -> gold lag <= 5 min).

Unlike a passive watermark reader, this probe MEASURES the real end-to-end
pipeline by driving it once per sample:

    1. INJECT a uniquely-identifiable "beacon" order into the SOURCE Postgres
       (a clean, accepted order — valid money, quantity > 0, status 'placed',
       a fresh ``ordered_at`` so it is never deduped or quarantined). Record
       ``t0`` = the source-arrival wall-clock (UTC) the instant after COMMIT.
    2. RUN C2 ingestion (Postgres -> ``raw.raw_orders`` and friends). The PK arm
       of C2 guarantees a brand-new ``order_id`` is captured regardless of the
       incremental watermark state.
    3. RUN ``dbt build`` (bronze -> silver -> gold) so the beacon propagates
       through the medallion into ``gold.gold_orders_obt``.
    4. POLL ``gold.gold_orders_obt`` (read-only) for the beacon ``order_id``.
       The instant it is queryable, record ``t1``.
    5. ``end_to_end_lag = t1 - t0`` for that sample.

Repeating gives a distribution; AC-3 is gated on the MEDIAN sample lag being
<= 5 minutes (300 s). The single-shot CI mode runs exactly one sample.

WHY ``order_id`` IS THE BEACON KEY
----------------------------------
``gold_orders_obt`` is at order grain and exposes ``order_id`` directly, so the
probe can match its own injected row exactly without any model change. The row's
freshly-stamped ``ordered_at`` also makes its duplicate-detection business key
unique, so the medallion classifier accepts it (never rejects/dedups it).

BOUNDARIES (R3 one-way dependency)
----------------------------------
The probe is a READ-ONLY consumer of the warehouse for verification and a
WRITER only of its own beacon rows into the SOURCE. On teardown it ALSO issues a
brief writable warehouse delete to purge its own beacons from ``raw``/``gold``
(C2 is upsert-only and never deletes, so a source-only cleanup would leak
synthetic beacons into gold forever — R7). It owns and removes exactly its own
beacon rows; it never imports Component B and never repairs real data.

CONCURRENCY (C4 contract)
-------------------------
C2 ingest and ``dbt build`` are the warehouse's two writers and MUST be
serialized (ingest THEN dbt). The probe runs them sequentially in exactly that
order. The gold poll uses a read-only connection and may run after the writers
close.
"""

from __future__ import annotations

import contextlib
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from platform.warehouse.connection import connect as warehouse_connect
from platform.warehouse.paths import warehouse_path_str
from typing import TYPE_CHECKING

import psycopg

from src.db.connection import conninfo

if TYPE_CHECKING:
    from collections.abc import Sequence

# --------------------------------------------------------------------------- #
# Constants                                                                    #
# --------------------------------------------------------------------------- #

# AC-3: source -> gold freshness budget. Median sample lag must be <= this.
AC3_BUDGET_S = 300.0  # 5 minutes

# Where the C3 dbt project lives, and the profiles dir within it.
_THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = _THIS_DIR.parent.parent
TRANSFORM_DIR = REPO_ROOT / "platform" / "transform"
PROFILES_DIR = TRANSFORM_DIR / "profiles"

# The beacon is a clean order: a valid status the gold layer keeps, positive
# money/quantity, and a fresh ordered_at so it is neither late-flagged nor
# deduped. customer_id / product_id are resolved live from the source.
BEACON_STATUS = "placed"
BEACON_QUANTITY = 1
BEACON_UNIT_PRICE = "9.99"
BEACON_TOTAL_AMOUNT = "9.99"

# Poll cadence while waiting for the beacon to appear in gold after dbt build.
GOLD_POLL_INTERVAL_S = 1.0
# Hard ceiling on a single sample so a stuck pipeline fails the gate, not hangs.
DEFAULT_SAMPLE_TIMEOUT_S = 600.0

# DuckDB single-writer lock contention markers. The C4 contract serializes
# writers, so the correct response to "another writer holds the file" is to WAIT
# and retry, not to fail the measurement. Every other writer path in the repo
# (evals, e2e tests) already does this; the probe matches them so a transient
# lock overlap does not turn a healthy pipeline into a false AC-3 breach.
_LOCK_CONFLICT_MARKERS = ("Could not set lock on file", "Conflicting lock is held")
# Number of times a writer step retries on a transient lock conflict, and the
# wait between attempts (~ WRITER_LOCK_RETRIES * WRITER_LOCK_WAIT_S ceiling).
WRITER_LOCK_RETRIES = 20
WRITER_LOCK_WAIT_S = 1.5


class ProbeError(RuntimeError):
    """Raised when the probe cannot run a measurement (env/pipeline failure)."""


def _is_lock_conflict(text: str) -> bool:
    return any(marker in text for marker in _LOCK_CONFLICT_MARKERS)


# --------------------------------------------------------------------------- #
# Result types                                                                 #
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class SampleResult:
    """One inject -> ingest -> dbt -> gold measurement."""

    order_id: int
    injected_at: datetime  # t0 (UTC) — source arrival
    visible_at: datetime  # t1 (UTC) — first queryable in gold
    end_to_end_lag_s: float  # t1 - t0
    ingest_s: float  # wall time spent in C2 ingestion
    dbt_s: float  # wall time spent in dbt build
    gold_wait_s: float  # poll time after dbt until the row appeared
    visible: bool  # True if the beacon reached gold within the timeout

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "injected_at": self.injected_at.isoformat(),
            "visible_at": self.visible_at.isoformat() if self.visible else None,
            "end_to_end_lag_s": round(self.end_to_end_lag_s, 3),
            "ingest_s": round(self.ingest_s, 3),
            "dbt_s": round(self.dbt_s, 3),
            "gold_wait_s": round(self.gold_wait_s, 3),
            "visible": self.visible,
        }


@dataclass(slots=True)
class ProbeRun:
    """The full set of samples for one probe invocation."""

    samples: list[SampleResult] = field(default_factory=list)
    beacon_order_ids: list[int] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Source-side beacon helpers                                                   #
# --------------------------------------------------------------------------- #


def _pick_fk_references(conn: psycopg.Connection) -> tuple[int, int]:
    """Return a valid (customer_id, product_id) for an FK-clean beacon order."""
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1")
        crow = cur.fetchone()
        cur.execute("SELECT product_id FROM products ORDER BY product_id LIMIT 1")
        prow = cur.fetchone()
    if crow is None or prow is None:
        raise ProbeError(
            "source has no customers/products — seed it first (make seed). "
            "The beacon order needs FK-valid references."
        )
    return int(crow[0]), int(prow[0])


def inject_beacon(conn: psycopg.Connection) -> tuple[int, datetime]:
    """Insert one clean beacon order into the SOURCE and return (order_id, t0).

    ``t0`` is captured immediately AFTER commit so the measured lag never
    understates source arrival. The row is intentionally ordinary so it survives
    the medallion into gold unmodified.
    """
    customer_id, product_id = _pick_fk_references(conn)
    ordered_at = datetime.now(UTC)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders "
            "(customer_id, product_id, quantity, unit_price, total_amount, status, ordered_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING order_id",
            (
                customer_id,
                product_id,
                BEACON_QUANTITY,
                BEACON_UNIT_PRICE,
                BEACON_TOTAL_AMOUNT,
                BEACON_STATUS,
                ordered_at,
            ),
        )
        order_id = int(cur.fetchone()[0])
    conn.commit()
    return order_id, datetime.now(UTC)


def remove_beacons(conn: psycopg.Connection, order_ids: Sequence[int]) -> int:
    """Delete the probe's own beacon orders from the SOURCE. Returns rows removed.

    Best-effort teardown so repeated probe runs do not accumulate synthetic rows
    in the source. Payments are never created for beacons, so there is no FK
    dependent to clear first.
    """
    ids = [int(i) for i in order_ids]
    if not ids:
        return 0
    with conn.cursor() as cur:
        cur.execute("DELETE FROM orders WHERE order_id = ANY(%s)", (ids,))
        removed = cur.rowcount
    conn.commit()
    return removed


def purge_beacons_from_warehouse(order_ids: Sequence[int]) -> int:
    """Remove the probe's beacon rows from raw + gold so the warehouse is pristine.

    R7 (chaos/probes are reversible): ``remove_beacons`` only clears the SOURCE,
    but C2 is upsert-only and NEVER deletes, so every beacon already ingested
    persists forever in ``raw.raw_orders`` AND propagates to
    ``gold.gold_orders_obt`` — each probe run would otherwise permanently inflate
    gold with synthetic orders and skew AC-2/AC-3 and any downstream eval. This
    mirrors the defect-survival eval's ``purge_raw_orphans``: after the source
    beacons are removed we delete the same order_ids from raw and gold directly.

    Uses a brief writable warehouse connection. The C4 single-writer contract is
    honoured because teardown runs AFTER all pipeline writers (ingest, dbt) for
    the run have closed. Best-effort: a failure here must not mask the
    measurement result, so callers wrap it accordingly.
    """
    ids = [int(i) for i in order_ids]
    if not ids:
        return 0
    removed = 0
    # Open the writable handle with the same transient-lock retry as the writers.
    conn = None
    for attempt in range(WRITER_LOCK_RETRIES + 1):
        try:
            conn = warehouse_connect(read_only=False)
            break
        except Exception as exc:  # noqa: BLE001 - retry only on a transient lock conflict
            if _is_lock_conflict(str(exc)) and attempt < WRITER_LOCK_RETRIES:
                time.sleep(WRITER_LOCK_WAIT_S)
                continue
            raise
    assert conn is not None
    with conn:
        present = {
            row[0]
            for row in conn.execute(
                "select table_name from information_schema.tables "
                "where table_schema in ('raw', 'gold')"
            ).fetchall()
        }
        # Delete the beacon order_ids from raw and gold (only tables that exist).
        # DuckDB's affected-row count on RETURNING-less DML is inconsistent, so we
        # do not read it here — `removed` is derived from a follow-up count below.
        for schema, table in (("raw", "raw_orders"), ("gold", "gold_orders_obt")):
            if table not in present:
                continue
            conn.execute(
                f"delete from {schema}.{table} where order_id = any(?)",  # noqa: S608 - static identifiers, bound param
                [ids],
            )
        # Report how many beacon rows remain (should be zero) for the caller's log.
        if "raw_orders" in present:
            remaining = conn.execute(
                "select count(*) from raw.raw_orders where order_id = any(?)",
                [ids],
            ).fetchone()[0]
            removed = len(ids) - int(remaining)
    return removed


# --------------------------------------------------------------------------- #
# Pipeline drivers (C2 ingest, then dbt build) — serialized per C4 contract    #
# --------------------------------------------------------------------------- #


def run_ingest() -> float:
    """Run C2 ingestion once (ephemeral full load) and return its wall time.

    An ephemeral instance is used deliberately: a single-shot probe must not
    depend on DAGSTER_HOME watermark state, and the raw upsert is idempotent so
    re-scanning everything is safe. The PK arm captures the new beacon order_id
    even if a persistent watermark were ahead of it.
    """
    # Import lazily so a probe that only reads gold does not require Dagster.
    from platform.ingestion.run import materialize_ingest

    start = time.monotonic()
    # materialize() raises on a failed asset op (raise_on_error defaults True), so
    # a single-writer lock conflict surfaces as an exception whose message carries
    # the DuckDB lock marker. Retry that as transient (C4 serializes writers);
    # any other exception or a non-success result is a real failure.
    for attempt in range(WRITER_LOCK_RETRIES + 1):
        try:
            result = materialize_ingest()
        except Exception as exc:  # noqa: BLE001 - inspect for a transient lock conflict only
            if _is_lock_conflict(str(exc)) and attempt < WRITER_LOCK_RETRIES:
                time.sleep(WRITER_LOCK_WAIT_S)
                continue
            raise ProbeError(f"C2 ingestion run failed; cannot measure freshness. ({exc})") from exc
        if result.success:
            return time.monotonic() - start
        raise ProbeError("C2 ingestion run failed (non-success); cannot measure freshness.")
    raise ProbeError("C2 ingestion run failed (lock contention did not clear); cannot measure freshness.")


def run_dbt_build() -> float:
    """Run ``dbt build`` over the medallion and return its wall time.

    Invoked as a subprocess (dbt's supported entrypoint) from the transform
    project dir, with DUCKDB_DATABASE pinned to the canonical warehouse path so
    dbt writes the SAME file C2 just wrote raw into.

    dbt is launched via ``<this interpreter> -m dbt.cli.main`` rather than a bare
    ``dbt`` so it resolves through the active virtualenv regardless of whether
    ``.venv/bin`` is on PATH (the probe is often run as ``python -m ...``).
    """
    import os
    import sys

    env = dict(os.environ)
    env["DUCKDB_DATABASE"] = warehouse_path_str()
    start = time.monotonic()
    # Retry only on a transient single-writer lock conflict (another writer holds
    # the warehouse file); the C4 contract serializes writers, so waiting is the
    # correct response. Any other non-zero exit fails immediately.
    for attempt in range(WRITER_LOCK_RETRIES + 1):
        proc = subprocess.run(  # noqa: S603 - fixed argv, no shell, env pinned
            [sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", str(PROFILES_DIR)],
            cwd=str(TRANSFORM_DIR),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return time.monotonic() - start
        tail = (proc.stdout or "") + (proc.stderr or "")
        if _is_lock_conflict(tail) and attempt < WRITER_LOCK_RETRIES:
            time.sleep(WRITER_LOCK_WAIT_S)
            continue
        raise ProbeError(
            f"dbt build failed (exit {proc.returncode}); cannot measure freshness.\n"
            f"{tail[-2000:]}"
        )
    return time.monotonic() - start


def _gold_has_order(order_id: int) -> bool:
    """Read-only check: is the beacon order_id queryable in gold yet?"""
    with warehouse_connect(read_only=True) as conn:
        row = conn.execute(
            "SELECT 1 FROM gold.gold_orders_obt WHERE order_id = ? LIMIT 1",
            [order_id],
        ).fetchone()
    return row is not None


def wait_for_gold(order_id: int, *, timeout_s: float) -> tuple[datetime, float]:
    """Poll gold until the beacon appears. Return (visible_at_utc, wait_seconds).

    Raises :class:`ProbeError` if the beacon does not appear within ``timeout_s``
    — a defect surviving (or being lost) through the medallion is a real AC-3
    failure, not a soft skip.
    """
    deadline = time.monotonic() + timeout_s
    start = time.monotonic()
    while True:
        if _gold_has_order(order_id):
            return datetime.now(UTC), time.monotonic() - start
        if time.monotonic() >= deadline:
            raise ProbeError(
                f"beacon order_id={order_id} did not reach gold.gold_orders_obt "
                f"within {timeout_s:.0f}s after dbt build — pipeline lost the row "
                f"or is stalled (AC-3 cannot be measured)."
            )
        time.sleep(GOLD_POLL_INTERVAL_S)


# --------------------------------------------------------------------------- #
# One sample, and the full run                                                 #
# --------------------------------------------------------------------------- #


def measure_once(*, sample_timeout_s: float = DEFAULT_SAMPLE_TIMEOUT_S) -> SampleResult:
    """Drive one full inject -> ingest -> dbt -> gold measurement.

    The source connection is opened only to inject the beacon and is closed
    before the pipeline runs, so the probe holds no source transaction across
    ingestion.
    """
    with psycopg.connect(**conninfo(), autocommit=False) as src:
        order_id, t0 = inject_beacon(src)

    ingest_s = run_ingest()
    dbt_s = run_dbt_build()
    t1, gold_wait_s = wait_for_gold(order_id, timeout_s=sample_timeout_s)

    lag_s = (t1 - t0).total_seconds()
    return SampleResult(
        order_id=order_id,
        injected_at=t0,
        visible_at=t1,
        end_to_end_lag_s=lag_s,
        ingest_s=ingest_s,
        dbt_s=dbt_s,
        gold_wait_s=gold_wait_s,
        visible=True,
    )


def run_probe(
    *,
    samples: int = 3,
    sample_timeout_s: float = DEFAULT_SAMPLE_TIMEOUT_S,
    cleanup: bool = True,
) -> ProbeRun:
    """Run ``samples`` end-to-end measurements and return the collected run.

    Args:
        samples: number of beacons to drive through the pipeline. CI mode passes
            ``1`` (single-shot). The median across samples is the AC-3 statistic.
        sample_timeout_s: per-sample ceiling for the beacon to reach gold.
        cleanup: when True, delete all injected beacons from the source after
            the run (default). Set False to leave them for inspection.

    Raises:
        ProbeError: on any pipeline/precondition failure. Connectivity errors
            (DB down) propagate to the CLI which converts them to a SKIP.
    """
    if samples < 1:
        raise ProbeError("samples must be >= 1")

    run = ProbeRun()
    try:
        for _ in range(samples):
            sample = measure_once(sample_timeout_s=sample_timeout_s)
            run.samples.append(sample)
            run.beacon_order_ids.append(sample.order_id)
    finally:
        if cleanup and run.beacon_order_ids:
            # 1. Remove beacons from the SOURCE.
            try:
                with psycopg.connect(**conninfo(), autocommit=False) as src:
                    remove_beacons(src, run.beacon_order_ids)
            except psycopg.Error:
                # Teardown is best-effort; a failed cleanup must not mask the
                # measurement result (or its error).
                pass
            # 2. Purge the same beacons from raw + gold (C2 never deletes, so the
            #    source delete alone leaves them in the warehouse forever — R7).
            #    Best-effort: a teardown failure must not mask the measurement.
            with contextlib.suppress(Exception):
                purge_beacons_from_warehouse(run.beacon_order_ids)
    return run
