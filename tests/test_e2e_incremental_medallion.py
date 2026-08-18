"""End-to-end test of the INCREMENTAL medallion path (no --full-refresh).

This is the regression test for the Round-1 BLOCKER: the medallion's
defect-survival guarantee (U3: defects caught in *_rejects, never leaked to gold)
must hold on the path PRODUCTION actually runs — incremental C2 ingest +
incremental ``dbt build`` (NO ``--full-refresh``) — not only under the
full-refresh flag the other e2e/eval scripts force.

It exercises the two defect classes the incremental path used to silently miss:

  * late_arrival   — an order backdated 45 days. Its ordered_at is far in the
    past, but it is a brand-new PK with a current _ingested_at. It must be
    ACCEPTED-AND-FLAGGED is_late and reach gold (an `ordered_at > max(ordered_at)`
    silver filter would have dropped it).
  * destructive_fix — zeroes total_amount on the most-recent orders IN PLACE,
    WITHOUT moving ordered_at. C2's PK-recency refresh arm must re-extract those
    rows (fresh _ingested_at) and the silver _ingested_at predicate must
    re-classify them: the stale CLEAN versions must be REPLACED, the corrupted
    rows QUARANTINED into silver_orders_rejects, and NONE may reach gold.

WHY A PERSISTENT DAGSTER INSTANCE. Incremental extraction reads the prior
high-watermark from the last successful materialization, so the run must persist
events across the two ingests. The test drives ``platform.ingestion.run
--persistent`` against a TEMP DAGSTER_HOME so it is genuinely incremental (the
default ephemeral path is a full reload and would mask the BLOCKER). dbt is run
WITHOUT --full-refresh for the same reason.

ISOLATION / SAFETY. Like test_e2e_backbone, this MUTATES the live source DB and
the shared warehouse, so it is opt-in via RUN_E2E_BACKBONE and requires a
reachable Postgres + an initialized warehouse. The finally-block always restores
a clean baseline (reseed + flush raw + full-refresh) so the next run is
reproducible (R7).
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
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

_BASELINE_ORDERS = 5000
_SEED_ARGS = ["--customers", "500", "--products", "200", "--orders", "5000", "--seed", "42"]

_LOCK_CONFLICT_MARKERS = ("Could not set lock on file", "Conflicting lock is held")


# --------------------------------------------------------------------------- #
# Skip guards (identical contract to test_e2e_backbone)                       #
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
        reason="source Postgres not reachable; run `make up && make seed` to enable the incremental e2e test.",
    ),
    pytest.mark.skipif(
        not _WAREHOUSE_DB.exists(),
        reason=f"warehouse not initialized at {_WAREHOUSE_DB}; run `make ingest-once && make dbt-build` first.",
    ),
]


# --------------------------------------------------------------------------- #
# Subprocess helpers                                                          #
# --------------------------------------------------------------------------- #


def _env_with_warehouse(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env["DUCKDB_DATABASE"] = str(_WAREHOUSE_DB)
    env["PYTHONPATH"] = str(_REPO_ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.pop("DUCKDB_PATH", None)
    env.pop("WAREHOUSE_DB_PATH", None)
    if extra:
        env.update(extra)
    return env


def _is_lock_conflict(proc: subprocess.CompletedProcess[str]) -> bool:
    blob = f"{proc.stdout}\n{proc.stderr}"
    return any(marker in blob for marker in _LOCK_CONFLICT_MARKERS)


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    lock_retries: int = 0,
    retry_wait_s: float = 3.0,
) -> subprocess.CompletedProcess[str]:
    import time

    attempt = 0
    while True:
        proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
            cmd,
            cwd=str(cwd) if cwd else str(_REPO_ROOT),
            env=env or _env_with_warehouse(),
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
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"--- stdout ---\n{proc.stdout[-4000:]}\n--- stderr ---\n{proc.stderr[-4000:]}"
        )


def _seed_clean(*, truncate: bool) -> None:
    cmd = [sys.executable, "-m", "src.seed.seed", *_SEED_ARGS]
    if truncate:
        cmd.append("--truncate")
    _run(cmd)


def _ingest_persistent(dagster_home: Path) -> None:
    """Run C2 ingest with a PERSISTENT instance => genuinely incremental.

    Uses a temp DAGSTER_HOME so the high-watermark from the first ingest is read
    by the second (the default ephemeral path would full-reload and mask the
    BLOCKER this test guards).
    """
    env = _env_with_warehouse({"DAGSTER_HOME": str(dagster_home)})
    _run([sys.executable, "-m", "platform.ingestion.run", "--persistent"], env=env, lock_retries=10)


def _ingest_ephemeral() -> None:
    """Full-reload ingest (for the clean-baseline restore only)."""
    _run([sys.executable, "-m", "platform.ingestion.run"], lock_retries=10)


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
    conn = psycopg.connect(**conninfo())
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE injected_incidents RESTART IDENTITY")
        conn.commit()
    finally:
        conn.close()


def _source_max_order_id() -> int:
    conn = psycopg.connect(**conninfo())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(order_id) FROM orders")
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _source_recent_order_ids(limit: int) -> list[int]:
    conn = psycopg.connect(**conninfo())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT order_id FROM orders ORDER BY order_id DESC LIMIT %s", (limit,))
            return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _warehouse() -> duckdb.DuckDBPyConnection:
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
# The incremental orchestration fixture                                       #
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def incremental_run() -> Iterator[dict[str, object]]:
    """Drive the full INCREMENTAL chain once; yield context; restore at the end.

    Steps (all incremental — no --full-refresh except the final restore):
      1. clean baseline (reseed truncate) + clear ledger + flush raw.
      2. ingest #1 (persistent) + dbt build #1 (incremental, first run = full
         build) — establishes the clean watermark + clean gold.
      3. capture the gold total_amount of the rows destructive_fix will corrupt
         (so we can prove the stale CLEAN value was REPLACED, not left behind).
      4. inject late_arrival + destructive_fix into the SOURCE.
      5. ingest #2 (persistent => incremental) + dbt build #2 (INCREMENTAL).
      6. yield a context dict the assertions read.
    Restore: reseed + flush raw + ephemeral ingest + full-refresh dbt.
    """
    dagster_home = Path(tempfile.mkdtemp(prefix="dagster_home_inc_"))

    _seed_clean(truncate=True)
    _clear_incident_ledger()
    _truncate_raw()
    _ingest_persistent(dagster_home)
    _dbt_build(full_refresh=False)  # first incremental run = full build of empty tables

    # The destructive_fix targets the 300 most-recent order_ids. Snapshot their
    # CLEAN total_amount in gold BEFORE corruption so we can prove replacement.
    targeted = _source_recent_order_ids(300)
    reader = _warehouse()
    try:
        clean_nonzero = reader.execute(
            "SELECT count(*) FROM gold.gold_orders_obt "
            "WHERE order_id = ANY(?) AND total_amount > 0",
            [targeted],
        ).fetchone()[0]
    finally:
        reader.close()

    # Inject the two incremental-path defects. ORDER MATTERS: destructive_fix
    # corrupts the 300 highest order_ids (ORDER BY order_id DESC LIMIT 300), so it
    # must run BEFORE late_arrival — otherwise late_arrival's brand-new (highest)
    # PK would itself be caught and zeroed by destructive_fix, masking the
    # accepted-and-flagged late_arrival assertion.
    _inject("destructive_fix")
    _inject("late_arrival")
    late_order_id = _source_max_order_id()  # late_arrival inserted the newest PK last

    # Incremental ingest #2 + INCREMENTAL dbt build #2 (the path under test).
    _ingest_persistent(dagster_home)
    _dbt_build(full_refresh=False)

    context: dict[str, object] = {
        "late_order_id": late_order_id,
        "targeted_order_ids": targeted,
        "clean_nonzero_before": int(clean_nonzero),
    }
    try:
        yield context
    finally:
        _seed_clean(truncate=True)
        _clear_incident_ledger()
        _truncate_raw()
        _ingest_ephemeral()
        _dbt_build(full_refresh=True)
        # Best-effort temp-dir cleanup.
        import shutil

        shutil.rmtree(dagster_home, ignore_errors=True)


@pytest.fixture()
def reader(incremental_run: dict[str, object]) -> Iterator[duckdb.DuckDBPyConnection]:
    conn = _warehouse()
    try:
        yield conn
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# late_arrival on the incremental path: accepted, flagged is_late, in gold     #
# --------------------------------------------------------------------------- #


def test_late_arrival_reaches_gold_flagged_on_incremental_path(
    reader: duckdb.DuckDBPyConnection,
    incremental_run: dict[str, object],
) -> None:
    """The 45-day-backdated order must reach gold (NOT dropped by an event-time filter).

    Keying silver on _ingested_at (processing time) instead of ordered_at (event
    time) is what admits this row on the incremental path. It must be present in
    gold AND flagged is_late (ordered_at materially predates ingestion).
    """
    late_id = int(incremental_run["late_order_id"])

    rows = reader.execute(
        "SELECT order_id, is_late FROM gold.gold_orders_obt WHERE order_id = ?",
        [late_id],
    ).fetchall()
    assert len(rows) == 1, (
        f"late_arrival order_id={late_id} did NOT reach gold on the incremental path "
        f"(an event-time silver filter would drop it); rows found={len(rows)}"
    )
    assert rows[0][1] is True, f"late_arrival order_id={late_id} reached gold but is_late was not flagged"

    # And it must NOT be quarantined — late_arrival is accepted-and-flagged, not a reject.
    quarantined = reader.execute(
        "SELECT count(*) FROM silver.silver_orders_rejects WHERE order_id = ?",
        [late_id],
    ).fetchone()[0]
    assert quarantined == 0, f"late_arrival order_id={late_id} was wrongly rejected (must be accepted-and-flagged)"


# --------------------------------------------------------------------------- #
# destructive_fix on the incremental path: re-classified, quarantined, replaced #
# --------------------------------------------------------------------------- #


def test_destructive_fix_is_caught_on_incremental_path(
    reader: duckdb.DuckDBPyConnection,
    incremental_run: dict[str, object],
) -> None:
    """In-place UPDATE to recent rows must be re-extracted and quarantined.

    destructive_fix zeroes total_amount on the 300 most-recent orders WITHOUT
    moving ordered_at. The C2 PK-recency arm must re-extract them (fresh
    _ingested_at) and the silver _ingested_at predicate must re-classify them
    into silver_orders_rejects with reject_rule='destructive_fix'. This is the
    exact case the full-refresh-only proof used to hide.
    """
    caught = reader.execute(
        "SELECT count(*) FROM silver.silver_orders_rejects WHERE reject_rule = 'destructive_fix'"
    ).fetchone()[0]
    assert caught >= 1, (
        "destructive_fix was NOT caught on the incremental path: no "
        "silver_orders_rejects row with reject_rule='destructive_fix' "
        "(the in-place UPDATE was missed by extraction or by the silver filter)."
    )


def test_destructive_fix_corruption_absent_from_gold_incremental(
    reader: duckdb.DuckDBPyConnection,
) -> None:
    """No zeroed-total corrupted order may survive into gold on the incremental path."""
    leaked = reader.execute(
        "SELECT count(*) FROM gold.gold_orders_obt "
        "WHERE total_amount = 0 AND quantity * unit_price > 0"
    ).fetchone()[0]
    assert leaked == 0, (
        f"gold leaked {leaked} destructive_fix-corrupted rows (total_amount=0 with "
        "quantity*unit_price>0) past the silver quarantine on the incremental path."
    )


def test_destructive_fix_replaced_the_stale_clean_rows(
    reader: duckdb.DuckDBPyConnection,
    incremental_run: dict[str, object],
) -> None:
    """The previously-clean gold rows for the corrupted PKs must be REPLACED, not stale.

    Before injection these targeted order_ids had total_amount > 0 in gold. After
    the incremental rebuild they are corrupted (total_amount=0) and quarantined,
    so they must NO LONGER appear in gold with their old clean value. If the
    incremental filter had skipped them, the stale clean rows would still be in
    gold — this asserts delete+insert on _ingested_at actually evicted them.
    """
    targeted = list(incremental_run["targeted_order_ids"])
    clean_before = int(incremental_run["clean_nonzero_before"])
    assert clean_before > 0, "precondition: targeted rows should have been clean (nonzero) in gold before injection"

    # The targeted rows are now corrupted (total_amount=0) at the source, so they
    # are quarantined and must be ABSENT from gold. The stale clean copies must be gone.
    still_clean_in_gold = reader.execute(
        "SELECT count(*) FROM gold.gold_orders_obt WHERE order_id = ANY(?) AND total_amount > 0",
        [targeted],
    ).fetchone()[0]
    assert still_clean_in_gold < clean_before, (
        "incremental rebuild left STALE clean rows in gold for destructive_fix targets: "
        f"{still_clean_in_gold} still carry the old clean total (was {clean_before}); "
        "the _ingested_at predicate + delete+insert did not evict the replaced rows."
    )
