"""L1 unit tests for the read-only Sentinel tools (no LLM).

Each tool is exercised against a controlled fixture surface:
  I1 ReadDagsterLogs   -> a temp DAGSTER_HOME with a seeded BACKBONE_RUN_FAILURE line
  I2 ReadDbtRunResults -> a temp run_results.json with a failed node
  I3 ProfileRejects    -> a temp DuckDB warehouse with seeded silver_*_rejects rows
  I3 QueryDuckDB       -> allow-list enforcement against the same temp warehouse
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("sentinel.tools", reason="Sentinel tools not importable")

from sentinel.tools.logs import (  # noqa: E402
    FAILURE_LOG_PREFIX,
    ReadDagsterLogs,
    ReadDbtRunResults,
    parse_failure_line,
)
from sentinel.tools.warehouse import ProfileRejects, QueryDuckDB  # noqa: E402


# --- I1: Dagster log parsing -------------------------------------------------
def test_parse_failure_line_extracts_fields() -> None:
    line = (
        f"2026-06-29 12:00:00 - ERROR - {FAILURE_LOG_PREFIX} "
        "run_id=abc-123 job=backbone_end_to_end error=relation user_id does not exist"
    )
    parsed = parse_failure_line(line)
    assert parsed == {
        "run_id": "abc-123",
        "job": "backbone_end_to_end",
        "error": "relation user_id does not exist",
    }


def test_parse_failure_line_ignores_non_contract_lines() -> None:
    assert parse_failure_line("just a normal info log line") is None


def test_read_dagster_logs_finds_seeded_failure(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "dagster_home"
    log_dir = home / "logs"
    log_dir.mkdir(parents=True)
    (log_dir / "event.log").write_text(
        f"{FAILURE_LOG_PREFIX} run_id=run-9 job=backbone_end_to_end error=boom\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DAGSTER_HOME", str(home))

    out = json.loads(ReadDagsterLogs()._run(run_id="latest", timeout_seconds=3.0))
    assert out["found"] is True
    assert out["record"]["run_id"] == "run-9"
    assert out["record"]["error"] == "boom"


def test_read_dagster_logs_clean_home_reports_not_found(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "empty_home"
    (home / "logs").mkdir(parents=True)
    monkeypatch.setenv("DAGSTER_HOME", str(home))

    out = json.loads(ReadDagsterLogs()._run())
    assert out["found"] is False


# --- I2: dbt run_results parsing --------------------------------------------
def test_read_dbt_run_results_detects_failed_node(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "run_results.json").write_text(
        json.dumps(
            {
                "results": [
                    {"unique_id": "model.x.silver_orders", "status": "success", "message": "OK"},
                    {
                        "unique_id": "model.x.silver_orders_rejects",
                        "status": "error",
                        "message": "Binder Error: column user_id not found (schema_drift)",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    out = json.loads(ReadDbtRunResults()._run(target_dir=str(target)))
    assert out["found"] is True
    assert out["failed_nodes"][0]["node"] == "model.x.silver_orders_rejects"
    assert out["failed_nodes"][0]["status"] == "error"


def test_read_dbt_run_results_all_success(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "run_results.json").write_text(
        json.dumps({"results": [{"unique_id": "m", "status": "success", "message": "OK"}]}),
        encoding="utf-8",
    )
    out = json.loads(ReadDbtRunResults()._run(target_dir=str(target)))
    assert out["found"] is False
    assert out["total_nodes"] == 1


# --- I3: warehouse profiling against a temp DuckDB ---------------------------
def _seed_warehouse(db_path: Path) -> None:
    """Build a tiny warehouse mirroring the silver reject/flag contract."""
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(
            "CREATE TABLE silver.silver_orders_rejects "
            "(order_id BIGINT, reject_rule VARCHAR, reject_reason VARCHAR)"
        )
        con.execute(
            "INSERT INTO silver.silver_orders_rejects VALUES "
            "(1, 'negative_price', 'unit_price < 0'), "
            "(2, 'negative_price', 'unit_price < 0'), "
            "(3, 'missing_customer', 'customer_id is null')"
        )
        con.execute(
            "CREATE TABLE silver.silver_payments_rejects "
            "(payment_id BIGINT, reject_rule VARCHAR, reject_reason VARCHAR)"
        )
        con.execute(
            "INSERT INTO silver.silver_payments_rejects VALUES (1, 'orphan_payment', 'no order')"
        )
        con.execute(
            "CREATE TABLE silver.silver_orders "
            "(order_id BIGINT, ordered_at TIMESTAMP, is_late BOOLEAN, _schema_drift BOOLEAN)"
        )
        con.execute(
            "INSERT INTO silver.silver_orders VALUES "
            "(1, TIMESTAMP '2026-06-01 10:00:00', true, false), "
            "(2, TIMESTAMP '2026-06-29 10:00:00', false, false)"
        )
    finally:
        con.close()


@pytest.fixture()
def temp_warehouse(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "warehouse.duckdb"
    _seed_warehouse(db_path)
    monkeypatch.setenv("DUCKDB_DATABASE", str(db_path))
    return db_path


def test_profile_rejects_finds_negative_price(temp_warehouse: Path) -> None:
    out = json.loads(ProfileRejects()._run(failure_key="negative_price", sample_limit=2))
    assert out["found"] is True
    assert out["count"] == 2
    assert out["reject_rule"] == "negative_price"
    assert out["evidence_surface"] == "silver_orders_rejects"
    assert len(out["sample_rows"]) == 2


def test_profile_rejects_recurring_aliases_to_negative_price(temp_warehouse: Path) -> None:
    # recurring_incident probes the negative_price reject_rule.
    out = json.loads(ProfileRejects()._run(failure_key="recurring_incident"))
    assert out["found"] is True
    assert out["reject_rule"] == "negative_price"


def test_profile_rejects_orphan_payment_uses_payments_table(temp_warehouse: Path) -> None:
    out = json.loads(ProfileRejects()._run(failure_key="orphan_payment"))
    assert out["found"] is True
    assert out["evidence_surface"] == "silver_payments_rejects"


def test_profile_rejects_late_arrival_uses_flag(temp_warehouse: Path) -> None:
    out = json.loads(ProfileRejects()._run(failure_key="late_arrival"))
    assert out["found"] is True
    assert out["count"] == 1
    assert out["evidence_surface"] == "silver_orders.is_late"


def test_profile_rejects_absent_failure_reports_not_found(temp_warehouse: Path) -> None:
    out = json.loads(ProfileRejects()._run(failure_key="duplicate_order"))
    assert out["found"] is False
    assert out["count"] == 0


def test_profile_rejects_unknown_key_is_handled(temp_warehouse: Path) -> None:
    out = json.loads(ProfileRejects()._run(failure_key="not_a_real_failure"))
    assert out["found"] is False
    assert "no I3 detection probe" in out["detail"]


def test_query_duckdb_allow_list(temp_warehouse: Path) -> None:
    ok = json.loads(QueryDuckDB()._run(table="silver.silver_orders"))
    assert ok["ok"] is True
    assert ok["count"] == 2

    rejected = json.loads(QueryDuckDB()._run(table="information_schema.tables"))
    assert rejected["ok"] is False
    assert "not allow-listed" in rejected["detail"]
