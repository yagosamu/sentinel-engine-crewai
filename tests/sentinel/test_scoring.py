"""Offline unit tests for the Sentinel scoring oracle (legacy string API).

These pin the back-compat ``score_diagnosis(key) -> str`` shim that wraps the new
graded rubric. The mocked Postgres connection mirrors the I4 ledger shape so no
live database is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip(
    "sentinel.scoring",
    reason="Component B (Sentinel) scoring module not importable",
)

from sentinel.scoring import score_diagnosis  # noqa: E402


def _fake_conn(failure_key: str | None) -> MagicMock:
    """Build a mocked Postgres connection that returns one incident row."""
    cursor = MagicMock()
    cursor.fetchone.return_value = (failure_key,) if failure_key is not None else None
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


@patch("sentinel.scoring.session")
def test_score_diagnosis_returns_correct_on_match(mock_session: MagicMock) -> None:
    mock_session.return_value = _fake_conn("stale_inventory_cache")

    assert score_diagnosis("stale_inventory_cache") == "correct"


@patch("sentinel.scoring.session")
def test_score_diagnosis_returns_incorrect_on_mismatch(mock_session: MagicMock) -> None:
    mock_session.return_value = _fake_conn("stale_inventory_cache")

    assert score_diagnosis("wrong_failure_key") == "incorrect"


@patch("sentinel.scoring.session")
def test_score_diagnosis_returns_incorrect_when_ledger_empty(mock_session: MagicMock) -> None:
    mock_session.return_value = _fake_conn(None)

    assert score_diagnosis("any_key") == "incorrect"
