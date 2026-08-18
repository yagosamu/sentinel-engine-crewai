"""End-to-end backbone test (Component A, Layers 1->5) on the LIVE database.

This is the integration test that proves the whole deterministic chain hangs
together, not just each layer in isolation:

    seed clean baseline (L1)
      -> C2 Dagster ingest  Postgres -> raw.raw_*           (L2)
      -> inject 3 KNOWN failures via the generator            (L1 chaos)
      -> re-ingest (incremental upsert lands the defects)     (L2)
      -> dbt build  bronze -> silver(+_rejects) -> gold       (L3)
      -> ASSERT:
           (a) gold row counts are sane (clean by construction);
           (b) EACH injected defect landed in the CORRECT silver_<x>_rejects
               table with reject_rule == the generator failure_key  (U3
               defect-survival: caught, not dropped, not leaked to gold);
           (c) a C5 execute_analytical_query returns aggregates that tie back
               to a direct DuckDB roll-up of the same gold table.        (L5)
      -> RESTORE a clean baseline (reseed source + full-refresh warehouse) so
         the run is reproducible (R7: chaos is reversible).

WHY THE FAILURES CHOSEN. Three deterministic, single-row, two-surface defects:

  * negative_price   -> silver_orders_rejects   reject_rule='negative_price'
  * invalid_quantity -> silver_orders_rejects   reject_rule='invalid_quantity'
  * orphan_payment   -> silver_payments_rejects reject_rule='orphan_payment'

They map 1:1 to a reject_rule in macros/classify.sql, span TWO different rejects
tables (orders + payments), and never collide on the order classifier's
first-match priority. Volume/timing/state failures (volume_spike, late_arrival,
ambiguous_anomaly, schema_drift) are intentionally excluded: per the failure map
they are accepted-and-flagged or count-signal, not row rejects, so asserting a
rejects row for them would contradict the model.

DETERMINISM. silver_orders/silver_payments are incremental; this test runs
`dbt build --full-refresh` so every defect is re-classified from scratch against
the freshly ingested raw, independent of any prior watermark/warehouse state.
(The INCREMENTAL path is covered separately and is also defect-faithful — see
tests/test_e2e_incremental_medallion.py; full-refresh here is the stricter check,
not a requirement for correctness.)

ISOLATION. This test MUTATES the live source database and the shared warehouse
file. It is opt-in via the RUN_E2E_BACKBONE env flag (and requires a reachable
Postgres); without both it SKIPS with a reason — it never fakes green and never
silently clobbers a developer's working DB.

ASSUMPTIONS NAMED (U2/U3). U2: dbt never reads Postgres; the test injects into
the SOURCE, C2 lands raw, dbt mirrors raw->bronze. U3: the test asserts each
defect SURVIVES to its silver_<x>_rejects quarantine AND is ABSENT from gold —
the explicit "defect was caught, not dropped" contract.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
import pytest

from src.db.connection import conninfo

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb

# --------------------------------------------------------------------------- #
# Paths / constants                                                           #
# --------------------------------------------------------------------------- #

_REPO_ROOT = Path(__file__).resolve().parents[1]
_TRANSFORM_DIR = _REPO_ROOT / "platform" / "transform"
_WAREHOUSE_DB = _REPO_ROOT / "platform" / "warehouse" / "warehouse.duckdb"

# The clean baseline the seeder produces (matches Makefile defaults).
_BASELINE = {"customers": 500, "products": 200, "orders": 5000, "payments": 5000}
_SEED_ARGS = ["--customers", "500", "--products", "200", "--orders", "5000", "--seed", "42"]

# The three defects to inject -> the (rejects table, reject_rule) each must land in.
# This is the binding U3 contract for this test.
_INJECTIONS: tuple[tuple[str, str, str], ...] = (
    ("negative_price", "silver_orders_rejects", "negative_price"),
    ("invalid_quantity", "silver_orders_rejects", "invalid_quantity"),
    ("orphan_payment", "silver_payments_rejects", "orphan_payment"),
)


# --------------------------------------------------------------------------- #
# Skip guards — never run destructively (or fake green) without explicit opt-in #
# --------------------------------------------------------------------------- #


def _postgres_reachable() -> bool:
    try:
        conn = psycopg.connect(**conninfo(), connect_timeout=2)
    except Exception:  # noqa: BLE001 - any connect failure => skip, not fail
        return False
    conn.close()
    return True


_OPT_IN = os.environ.get("RUN_E2E_BACKBONE", "").strip().lower() in {"1", "true", "yes", "on"}

pytestmark = [
    pytest.mark.skipif(
        not _OPT_IN,
        reason=(
            "destructive live-DB e2e is opt-in: set RUN_E2E_BACKBONE=1 to run "
            "(it mutates the source DB and the shared warehouse, then restores)."
        ),
    ),
    pytest.mark.skipif(
        not _postgres_reachable(),
        reason="source Postgres not reachable; run `make up && make seed` to enable the e2e backbone test.",
    ),
    pytest.mark.skipif(
        not _WAREHOUSE_DB.exists(),
        reason=f"warehouse not initialized at {_WAREHOUSE_DB}; run `make ingest-once && make dbt-build` first.",
    ),
]


# --------------------------------------------------------------------------- #
# Subprocess helpers (each layer driven the same way the Makefile drives it)  #
# --------------------------------------------------------------------------- #


def _env_with_warehouse() -> dict[str, str]:
    """Env that pins every layer to the ONE canonical warehouse file (C4)."""
    env = dict(os.environ)
    env["DUCKDB_DATABASE"] = str(_WAREHOUSE_DB)
    env["PYTHONPATH"] = str(_REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    # Drop the rejected path aliases so paths.py resolves the canonical one only.
    env.pop("DUCKDB_PATH", None)
    env.pop("WAREHOUSE_DB_PATH", None)
    return env


# Substrings that mark a transient DuckDB single-writer lock conflict (another
# writer — the C4 contract serializes writers, so the correct response is to WAIT
# and retry, not to fail). Used to make warehouse-writing steps resilient to a
# concurrent writer briefly holding the file lock.
_LOCK_CONFLICT_MARKERS = ("Could not set lock on file", "Conflicting lock is held")


def _is_lock_conflict(proc: subprocess.CompletedProcess[str]) -> bool:
    blob = f"{proc.stdout}\n{proc.stderr}"
    return any(marker in blob for marker in _LOCK_CONFLICT_MARKERS)


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    lock_retries: int = 0,
    retry_wait_s: float = 3.0,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, surfacing stdout/stderr on failure (no silent green).

    For warehouse writers, ``lock_retries`` > 0 retries ONLY on a transient
    DuckDB single-writer lock conflict (another writer holds the file). Any other
    non-zero exit fails immediately with full output captured.
    """
    import time

    attempt = 0
    while True:
        proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
            cmd,
            cwd=str(cwd) if cwd else str(_REPO_ROOT),
            env=_env_with_warehouse(),
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        if proc.returncode == 0:
            return proc
        if attempt < lock_retries and _is_lock_conflict(proc):
            attempt += 1
            time.sleep(retry_wait_s)
            continue
        raise AssertionError(
            f"command failed ({proc.returncode}) after {attempt} lock-retr"
            f"{'y' if attempt == 1 else 'ies'}: {' '.join(cmd)}\n"
            f"--- stdout ---\n{proc.stdout[-4000:]}\n--- stderr ---\n{proc.stderr[-4000:]}"
        )


def _seed_clean(*, truncate: bool) -> None:
    cmd = [sys.executable, "-m", "src.seed.seed", *_SEED_ARGS]
    if truncate:
        cmd.append("--truncate")
    _run(cmd)


def _ingest() -> None:
    # Ephemeral instance => cold full load; raw upserts by PK so already-present
    # rows are untouched and the freshly injected defect rows are landed.
    # Writer => tolerate a concurrent writer briefly holding the lock.
    _run([sys.executable, "-m", "platform.ingestion.run"], lock_retries=10)


# A tiny program (run as its OWN process) that truncates raw.raw_*. Running it in
# a separate, fully-exiting process means the DuckDB single-writer lock is taken
# and RELEASED before the ingest subprocess opens its own writable handle — no
# in-pytest-process handle can linger and contend with the writer (which on macOS
# surfaces as an IOException lock conflict / an interrupted ingest).
_TRUNCATE_RAW_PROG = """
from platform.warehouse.connection import connection
with connection(read_only=False) as conn:
    present = {
        r[0]
        for r in conn.execute(
            "select table_name from information_schema.tables where table_schema = 'raw'"
        ).fetchall()
    }
    for name in ("raw_customers", "raw_products", "raw_orders", "raw_payments"):
        if name in present:
            conn.execute("TRUNCATE raw." + name)
print("raw truncated:", sorted(present))
"""


def _truncate_raw() -> None:
    """Empty the raw.raw_* tables so the next ingest is a TRUE mirror of source.

    C2 ingest is upsert-only (it never DELETEs), so rows removed from the source
    — e.g. by our truncating reseed — would otherwise survive in raw indefinitely
    and propagate all the way to gold (making gold larger than the source, which
    the sanity test correctly rejects). Run as a separate process so the
    single-writer lock is released before the ingest writer opens its handle.
    """
    _run([sys.executable, "-c", _TRUNCATE_RAW_PROG], lock_retries=10)


def _inject(failure_key: str) -> None:
    _run([sys.executable, "-m", "src.gen.cli", "inject", failure_key])


def _dbt_build(*, full_refresh: bool) -> None:
    cmd = [sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", "profiles"]
    if full_refresh:
        cmd.append("--full-refresh")
    _run(cmd, cwd=_TRANSFORM_DIR, lock_retries=10)


# --------------------------------------------------------------------------- #
# DB read helpers                                                             #
# --------------------------------------------------------------------------- #


def _clear_incident_ledger() -> None:
    """Zero the injected_incidents ground-truth ledger (the I4 scoring oracle).

    The seeder's truncate_all deliberately PRESERVES injected_incidents (it is
    meant to accumulate ground truth across chaos runs). For this test to have a
    deterministic, reproducible clean baseline we explicitly clear it here, both
    before the run and during restore.
    """
    conn = psycopg.connect(**conninfo())
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE injected_incidents RESTART IDENTITY")
        conn.commit()
    finally:
        conn.close()


def _source_count(table: str) -> int:
    conn = psycopg.connect(**conninfo())
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - static identifier
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _warehouse() -> duckdb.DuckDBPyConnection:
    """Open a read-only warehouse handle, retrying past a transient writer lock.

    A read-only open can momentarily fail if a writer holds the file; the C4
    contract permits readers alongside one writer, so we briefly retry rather
    than fail on contention.
    """
    import time
    from platform.warehouse.connection import connect_read_only

    last_exc: Exception | None = None
    for _ in range(10):
        try:
            return connect_read_only()
        except Exception as exc:  # noqa: BLE001 - retry only on a lock conflict
            if not any(m in str(exc) for m in _LOCK_CONFLICT_MARKERS):
                raise
            last_exc = exc
            time.sleep(2.0)
    raise AssertionError(f"could not open read-only warehouse after retries: {last_exc}")


# --------------------------------------------------------------------------- #
# The orchestration fixture: drive the WHOLE chain once, restore at the end   #
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def backbone_run() -> Iterator[dict[str, object]]:
    """Run the full inject->detect chain once; yield a context; then restore.

    Yields a dict the assertion tests read so they share ONE expensive build.
    The finally-block ALWAYS reseeds the source and full-refreshes the warehouse
    so the next developer / next run starts from a clean baseline (R7).
    """
    # 1. Clean baseline in the source (truncate => deterministic 5000 orders).
    #    Also zero the incident ledger, which the seeder intentionally preserves.
    _seed_clean(truncate=True)
    _clear_incident_ledger()
    assert _source_count("orders") == _BASELINE["orders"], "baseline seed did not produce 5000 orders"
    assert _source_count("injected_incidents") == 0, "baseline should have no injected incidents"

    # 2. Flush raw, then ingest the clean baseline so raw mirrors source
    #    pre-chaos (C2 never deletes; clearing raw makes the mirror faithful and
    #    the run independent of any stale rows a prior run left behind).
    _truncate_raw()
    _ingest()

    # 3. Inject the three known failures into the SOURCE (each logs ground truth).
    for failure_key, _table, _rule in _INJECTIONS:
        _inject(failure_key)
    incidents = _source_count("injected_incidents")
    assert incidents == len(_INJECTIONS), f"expected {len(_INJECTIONS)} incident ledger rows, got {incidents}"

    # 4. Re-ingest so the new defect rows land in raw.
    _ingest()

    # 5. Transform: bronze -> silver(+rejects) -> gold. Full-refresh => every
    #    defect is re-classified from scratch (incremental models reset).
    _dbt_build(full_refresh=True)

    context: dict[str, object] = {"injections": _INJECTIONS, "incidents": incidents}
    try:
        yield context
    finally:
        # RESTORE: clean source + clean warehouse, so the run is reproducible.
        # Flush raw so the restored warehouse mirrors the 5000-order baseline
        # exactly (no defect rows, no stranded upserts) — gold == source again.
        # Also clear the incident ledger so the next run starts from zero.
        _seed_clean(truncate=True)
        _clear_incident_ledger()
        _truncate_raw()
        _ingest()
        _dbt_build(full_refresh=True)


@pytest.fixture()
def reader(backbone_run: dict[str, object]) -> Iterator[duckdb.DuckDBPyConnection]:
    conn = _warehouse()
    try:
        yield conn
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# (a) gold row counts are sane                                                #
# --------------------------------------------------------------------------- #


def test_gold_row_counts_are_sane(reader: duckdb.DuckDBPyConnection) -> None:
    """gold is non-empty, at order grain, and within [accepted, source] bounds.

    gold_orders_obt is built over silver-ACCEPTED orders only, so its count must
    be > 0, <= the source order count, and EXACTLY the silver_orders count (the
    OBT left-joins dims/payments at order grain — no fan-out, no drop).
    """
    obt = reader.execute("SELECT count(*) FROM gold.gold_orders_obt").fetchone()[0]
    silver_orders = reader.execute("SELECT count(*) FROM silver.silver_orders").fetchone()[0]
    source_orders = _source_count("orders")

    assert obt > 0, "gold_orders_obt is empty — the chain produced no accepted orders"
    assert obt == silver_orders, f"gold OBT {obt} != silver_orders {silver_orders} (grain/fan-out bug)"
    assert obt <= source_orders, f"gold OBT {obt} exceeds source orders {source_orders} (impossible)"

    # order_id is the grain: it must be unique in the OBT.
    dups = reader.execute(
        "SELECT count(*) - count(DISTINCT order_id) FROM gold.gold_orders_obt"
    ).fetchone()[0]
    assert dups == 0, f"gold_orders_obt has {dups} duplicate order_id (grain violated)"

    # gold_revenue_daily must also be populated and carry only non-negative money
    # (negative-price defects were quarantined upstream, so gold money is clean).
    rev_rows = reader.execute("SELECT count(*) FROM gold.gold_revenue_daily").fetchone()[0]
    assert rev_rows > 0, "gold_revenue_daily is empty"
    neg = reader.execute(
        "SELECT count(*) FROM gold.gold_revenue_daily WHERE gross_revenue < 0"
    ).fetchone()[0]
    assert neg == 0, f"gold_revenue_daily has {neg} negative-revenue days (defect leaked past silver)"


# --------------------------------------------------------------------------- #
# (b) U3 defect-survival: each defect landed in the right rejects table AND    #
#     is absent from gold                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(("failure_key", "rejects_table", "reject_rule"), _INJECTIONS)
def test_injected_defect_lands_in_correct_rejects_table(
    reader: duckdb.DuckDBPyConnection,
    failure_key: str,
    rejects_table: str,
    reject_rule: str,
) -> None:
    """Each injected failure must appear in its silver_<x>_rejects table.

    This is the U3 'caught, not dropped' assertion: the defect SURVIVED the
    medallion as a quarantined row keyed by reject_rule == the generator
    failure_key, so a downstream eval (or the Sentinel) can prove it was caught.
    """
    caught = reader.execute(
        f"SELECT count(*) FROM silver.{rejects_table} WHERE reject_rule = ?",  # noqa: S608 - static table, bound param
        [reject_rule],
    ).fetchone()[0]
    assert caught >= 1, (
        f"defect {failure_key!r} was NOT caught: silver.{rejects_table} has no row with "
        f"reject_rule={reject_rule!r} (U3 defect-survival failed — defect dropped or leaked)"
    )

    # And the reject_reason is populated (the quarantine row is inspectable).
    blank_reasons = reader.execute(
        f"SELECT count(*) FROM silver.{rejects_table} "  # noqa: S608 - static table, bound param
        "WHERE reject_rule = ? AND (reject_reason IS NULL OR trim(reject_reason) = '')",
        [reject_rule],
    ).fetchone()[0]
    assert blank_reasons == 0, f"{rejects_table} rows for {reject_rule!r} have blank reject_reason"


def test_negative_price_defect_absent_from_gold(reader: duckdb.DuckDBPyConnection) -> None:
    """The negative-price order must NOT reach gold (quarantined, not leaked)."""
    leaked = reader.execute(
        "SELECT count(*) FROM gold.gold_orders_obt WHERE unit_price < 0 OR total_amount < 0"
    ).fetchone()[0]
    assert leaked == 0, f"gold_orders_obt leaked {leaked} negative-money rows past the silver quarantine"


def test_invalid_quantity_defect_absent_from_gold(reader: duckdb.DuckDBPyConnection) -> None:
    """The non-positive-quantity order must NOT reach gold."""
    leaked = reader.execute(
        "SELECT count(*) FROM gold.gold_orders_obt WHERE quantity IS NULL OR quantity <= 0"
    ).fetchone()[0]
    assert leaked == 0, f"gold_orders_obt leaked {leaked} non-positive-quantity rows past the quarantine"


def test_orphan_payment_defect_absent_from_gold_payment_rollup(
    reader: duckdb.DuckDBPyConnection,
) -> None:
    """The orphan payment (order_id=999999999) must not appear in any gold payment rollup."""
    leaked = reader.execute(
        "SELECT count(*) FROM gold.gold_orders_obt WHERE order_id = 999999999"
    ).fetchone()[0]
    assert leaked == 0, "the orphan payment's bogus order_id leaked into gold"


# --------------------------------------------------------------------------- #
# (c) C5 MCP execute_analytical_query returns correct aggregates              #
# --------------------------------------------------------------------------- #


def test_mcp_query_aggregates_match_direct_gold_rollup(
    reader: duckdb.DuckDBPyConnection,
) -> None:
    """C5 execute_analytical_query must tie back to a direct gold roll-up.

    We run the 'order_status_breakdown' intent through the real C5 engine and
    reconcile every (status -> order_count) pair against an independent GROUP BY
    over the SAME gold table. Equality proves the intelligence layer reports what
    actually landed in gold (R5: Component A verified by assertion), end to end.
    """
    from platform.intelligence.query_engine import execute_analytical_query

    # Ground truth: direct roll-up over gold (the engine reads the same table).
    direct = {
        status: int(count)
        for status, count in reader.execute(
            "SELECT status, count(*) FROM gold.gold_orders_obt GROUP BY status"
        ).fetchall()
    }
    assert direct, "no orders in gold to aggregate"

    result = execute_analytical_query("order_status_breakdown", limit=100)
    via_mcp = {row["status"]: int(row["order_count"]) for row in result["rows"]}

    assert via_mcp == direct, (
        "C5 order_status_breakdown disagrees with a direct gold roll-up:\n"
        f"  via C5:  {via_mcp}\n  direct:  {direct}"
    )

    # And the grand total reconciles with the OBT row count (no rows dropped).
    obt = reader.execute("SELECT count(*) FROM gold.gold_orders_obt").fetchone()[0]
    assert sum(via_mcp.values()) == obt, "C5 status counts do not sum to the gold OBT row count"


def test_mcp_revenue_by_category_is_nonnegative_and_consistent(
    reader: duckdb.DuckDBPyConnection,
) -> None:
    """revenue_by_category via C5 must reconcile with gold_revenue_daily roll-up.

    Defects were quarantined, so every category's realized revenue is >= 0 and
    matches an independent sum over the same gold table.
    """
    from platform.intelligence.query_engine import execute_analytical_query

    direct = {
        cat: str(rev)
        for cat, rev in reader.execute(
            "SELECT product_category, sum(gross_revenue) "
            "FROM gold.gold_revenue_daily GROUP BY product_category"
        ).fetchall()
    }
    assert direct, "no revenue rows in gold_revenue_daily"

    result = execute_analytical_query("revenue_by_category", limit=100)
    via_mcp = {row["product_category"]: row["gross_revenue"] for row in result["rows"]}

    assert set(via_mcp) == set(direct), (
        f"category sets differ: C5={sorted(via_mcp)} direct={sorted(direct)}"
    )
    for cat, rev in via_mcp.items():
        # Money is carried as a string (no float rounding); compare via Decimal.
        from decimal import Decimal

        assert Decimal(rev) == Decimal(direct[cat]), (
            f"category {cat!r}: C5 revenue {rev} != direct {direct[cat]}"
        )
        assert Decimal(rev) >= 0, f"category {cat!r} has negative revenue {rev} (defect leaked)"
