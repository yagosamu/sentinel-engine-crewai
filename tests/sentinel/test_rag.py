"""L1 tests for the I5 incident RAG (ambiguous_anomaly Knowledge + recurrence).

The RAG store is Component B's own artefact: it cold-starts empty and bootstraps
from the I4 ledger (read-only). The ranking is a pure token-overlap function tested
here without a live DB; the ledger read is exercised through a mocked connection,
mirroring the scoring-test pattern.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("sentinel.tools.rag", reason="Sentinel RAG tool not importable")

from sentinel.tools.rag import QueryIncidentRAG, _Incident, rank_incidents  # noqa: E402


def test_rank_incidents_orders_by_overlap() -> None:
    incidents = [
        _Incident("negative_price", "unit_price below zero", "Data Profiler", "t1"),
        _Incident("missing_customer", "customer_id is null", "Data Profiler", "t2"),
    ]
    ranked = rank_incidents("negative unit_price detected", incidents, top_k=2)
    assert ranked[0][1].failure_key == "negative_price"  # best overlap first


def test_rank_incidents_drops_zero_overlap() -> None:
    incidents = [_Incident("orphan_payment", "payment without an order", "Data Profiler", "t1")]
    ranked = rank_incidents("schema drift in dbt", incidents, top_k=5)
    assert ranked == []  # no shared tokens


def _ledger_conn(rows: list[tuple]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_query_rag_cold_start_is_empty() -> None:
    with patch("sentinel.tools.rag.session", return_value=_ledger_conn([])):
        out = json.loads(QueryIncidentRAG()._run(description="anything"))
    assert out["found"] is False
    assert "cold start" in out["detail"]


def test_query_rag_reports_recurrence_count() -> None:
    rows = [
        ("negative_price", "unit_price below zero", "Data Profiler", "t3"),
        ("negative_price", "unit_price below zero", "Data Profiler", "t2"),
        ("missing_customer", "null customer", "Data Profiler", "t1"),
    ]
    with patch("sentinel.tools.rag.session", return_value=_ledger_conn(rows)):
        out = json.loads(QueryIncidentRAG()._run(description="negative unit_price"))
    assert out["found"] is True
    # recurring_incident's data side: "seen N times before".
    assert out["recurrence"]["failure_key"] == "negative_price"
    assert out["recurrence"]["times_seen"] == 2
