"""E2E tests for C2 ingestion (Postgres -> DuckDB raw.raw_*).

These exercise the real extraction path against the seeded source database, so
they require a reachable, seeded Postgres (``make up && make seed``). When the
source is unreachable the whole module is skipped rather than failing — unit
runners without Docker stay green.

Materialization is expensive (a full load mirrors every source row), so a
module-scoped fixture materializes ONCE into a shared temp warehouse and the
read-only assertions reuse it. The idempotency test has its own fixture because
it legitimately needs two successive runs against one instance.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
import pytest
from dagster import DagsterInstance

from src.db.connection import conninfo

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb


def _postgres_reachable() -> bool:
    try:
        conn = psycopg.connect(**conninfo(), connect_timeout=2)
    except Exception:  # noqa: BLE001 - any connect failure => skip
        return False
    conn.close()
    return True


pytestmark = pytest.mark.skipif(
    not _postgres_reachable(),
    reason="source Postgres not reachable; run `make up && make seed` to enable C2 e2e tests",
)


def _source_count(table: str) -> int:
    conn = psycopg.connect(**conninfo())
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - static table name
            return cur.fetchone()[0]
    finally:
        conn.close()


@pytest.fixture(scope="module")
def loaded_warehouse(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Materialize all four raw assets ONCE (cold start) into a temp warehouse."""
    import os

    db = tmp_path_factory.mktemp("c2") / "warehouse.duckdb"
    prev = os.environ.get("DUCKDB_DATABASE")
    os.environ["DUCKDB_DATABASE"] = str(db)
    os.environ.pop("DUCKDB_PATH", None)
    os.environ.pop("WAREHOUSE_DB_PATH", None)
    try:
        from platform.ingestion.assets import ALL_ASSETS
        from platform.ingestion.run import materialize_ingest

        # Scope to the four raw assets ONLY (the explicit-subset path). These are
        # C2 ingestion tests (raw.raw_* invariants); materialize_ingest()'s
        # DEFAULT target is now the full 18-asset graph (raw + dbt) for the
        # end-to-end CLI, but a C2 unit test must not pull in the C3 dbt build —
        # that would couple C2 verification to C3 and make these tests slow.
        with DagsterInstance.ephemeral() as instance:
            result = materialize_ingest(assets=ALL_ASSETS, instance=instance)
        assert result.success, "cold-start materialization failed"
        yield db
    finally:
        if prev is None:
            os.environ.pop("DUCKDB_DATABASE", None)
        else:
            os.environ["DUCKDB_DATABASE"] = prev


@pytest.fixture()
def reader(loaded_warehouse: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    from platform.warehouse.connection import connect_read_only

    conn = connect_read_only()
    try:
        yield conn
    finally:
        conn.close()


def test_full_load_mirrors_source_row_counts(reader: duckdb.DuckDBPyConnection) -> None:
    """Every source row lands in raw.raw_* (schema-qualified, prefixed names)."""
    for entity in ("customers", "products", "orders", "payments"):
        n_raw = reader.execute(f"SELECT count(*) FROM raw.raw_{entity}").fetchone()[0]
        n_source = _source_count(entity)
        assert n_raw == n_source, f"raw.raw_{entity} {n_raw} != source {n_source}"


def test_ingested_at_stamped_on_every_row(reader: duckdb.DuckDBPyConnection) -> None:
    """_ingested_at (the AC-3 anchor) is non-null on every raw row."""
    for entity in ("customers", "products", "orders", "payments"):
        nulls = reader.execute(f"SELECT count(*) FROM raw.raw_{entity} WHERE _ingested_at IS NULL").fetchone()[0]
        assert nulls == 0, f"raw.raw_{entity} has {nulls} null _ingested_at"


def test_money_is_decimal_timestamps_are_tz_aware(reader: duckdb.DuckDBPyConnection) -> None:
    """unit_price/total_amount are DECIMAL; time columns are TIMESTAMP WITH TIME ZONE."""
    types = dict(
        reader.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='raw' AND table_name='raw_orders'"
        ).fetchall()
    )
    assert types["unit_price"].startswith("DECIMAL")
    assert types["total_amount"].startswith("DECIMAL")
    assert "TIME ZONE" in types["ordered_at"]
    assert "TIME ZONE" in types["_ingested_at"]


def test_schema_drift_column_present_and_customer_slot_populated(reader: duckdb.DuckDBPyConnection) -> None:
    """raw_orders carries _schema_drift; the customer_id slot is resolved (not all-null)."""
    cols = [
        r[0]
        for r in reader.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='raw' AND table_name='raw_orders'"
        ).fetchall()
    ]
    assert "_schema_drift" in cols
    total = reader.execute("SELECT count(*) FROM raw.raw_orders").fetchone()[0]
    null_customer = reader.execute("SELECT count(*) FROM raw.raw_orders WHERE customer_id IS NULL").fetchone()[0]
    # A broken drift-slot mapping would null the WHOLE column; assert it did not.
    assert null_customer < total


def test_incremental_run_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two successive materializations against one instance produce no duplicate PKs."""
    db = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("DUCKDB_DATABASE", str(db))
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.delenv("WAREHOUSE_DB_PATH", raising=False)

    from platform.ingestion.assets import ALL_ASSETS
    from platform.ingestion.run import materialize_ingest
    from platform.warehouse.connection import connect_read_only

    home = tmp_path / "dagster_home"
    home.mkdir(parents=True, exist_ok=True)
    # Raw-only subset: this asserts incremental/idempotent watermark behaviour of
    # the C2 raw assets across two runs; the dbt step is irrelevant here and the
    # explicit-subset path keeps the test fast and C2-scoped.
    with DagsterInstance.from_ref(_temp_instance_ref(home)) as instance:
        assert materialize_ingest(assets=ALL_ASSETS, instance=instance).success
        assert materialize_ingest(assets=ALL_ASSETS, instance=instance).success

    conn = connect_read_only()
    try:
        for entity, pk in (
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("orders", "order_id"),
            ("payments", "payment_id"),
        ):
            dups = conn.execute(
                f"SELECT count(*) - count(DISTINCT {pk}) FROM raw.raw_{entity}"  # noqa: S608 - static identifiers
            ).fetchone()[0]
            assert dups == 0, f"raw.raw_{entity} has {dups} duplicate {pk}"
    finally:
        conn.close()


def _temp_instance_ref(home: Path):
    """Build a local DagsterInstance ref backed by a temp home for cross-run state."""
    from dagster._core.instance import InstanceRef

    return InstanceRef.from_dir(str(home))
