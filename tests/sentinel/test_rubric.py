"""L1 unit tests for the graded scoring rubric (no LLM, no live DB).

Every tier of ``score_run`` is exercised against a mocked I4 ledger: exact,
alias, cascade partial-credit, miss, the non-gating evidence dimension, and the
NO-RUN honesty guarantee.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("sentinel.scoring", reason="Sentinel scoring not importable")

from sentinel.models import Diagnosis  # noqa: E402
from sentinel.scoring import (  # noqa: E402
    ALIAS_SCORE,
    CASCADE_BASE,
    EXACT_SCORE,
    MISS_SCORE,
    ScoreResult,
    score_run,
)

_SINCE = datetime(2026, 1, 1, tzinfo=UTC)


def _ledger_conn(rows: list[str]) -> MagicMock:
    """Mock a psycopg connection whose cursor returns the given failure_keys.

    ``fetchall`` returns all rows (the run-window path); ``fetchone`` returns the
    last row as a 1-tuple (the latest-row path).
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = [(k,) for k in rows]
    cursor.fetchone.return_value = (rows[-1],) if rows else None
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _score(diagnosis: Diagnosis, ledger: list[str], since=_SINCE) -> ScoreResult:
    with patch("sentinel.scoring.session", return_value=_ledger_conn(ledger)):
        return score_run(diagnosis, since=since)


def test_exact_match_scores_one() -> None:
    r = _score(
        Diagnosis(failure_key="negative_price", evidence_surface="silver_orders_rejects"),
        ["negative_price"],
    )
    assert r.tier == "exact"
    assert r.diagnosis_score == EXACT_SCORE
    assert r.evidence_score == 1.0  # correct surface cited
    assert r.is_correct


def test_exact_match_with_wrong_evidence_is_flagged_not_rewarded() -> None:
    # HONESTY RULE: a right key with fabricated/missing evidence -> match 1.0,
    # evidence 0.0, visibly flagged.
    r = _score(
        Diagnosis(failure_key="negative_price", evidence_surface="totally_made_up"),
        ["negative_price"],
    )
    assert r.diagnosis_score == EXACT_SCORE
    assert r.evidence_score == 0.0


def test_alias_recurring_to_negative_price() -> None:
    # The recurring_incident injector writes negative_price rows; the oracle row
    # is negative_price, the crew said recurring_incident -> aliased credit.
    r = _score(
        Diagnosis(failure_key="recurring_incident", evidence_surface="silver_orders_rejects"),
        ["negative_price"],
    )
    assert r.tier == "alias"
    assert r.diagnosis_score == ALIAS_SCORE
    assert not r.is_correct  # alias is NOT a string-API "correct"


def test_miss_scores_zero() -> None:
    r = _score(
        Diagnosis(failure_key="duplicate_order", evidence_surface="silver_orders_rejects"),
        ["missing_customer"],
    )
    assert r.tier == "miss"
    assert r.diagnosis_score == MISS_SCORE


def test_cascade_full_overlap() -> None:
    members = ["missing_customer", "volume_spike", "schema_drift"]
    ledger = [*members, "multi_failure_cascade"]
    r = _score(
        Diagnosis(failure_key="multi_failure_cascade", sub_failures=members),
        ledger,
    )
    assert r.tier == "cascade"
    assert r.diagnosis_score == pytest.approx(1.0)  # base 0.5 + 0.5*(3/3)


def test_cascade_partial_overlap() -> None:
    members = ["missing_customer", "volume_spike", "schema_drift"]
    ledger = [*members, "multi_failure_cascade"]
    r = _score(
        Diagnosis(failure_key="multi_failure_cascade", sub_failures=["missing_customer"]),
        ledger,
    )
    # base 0.5 + 0.5*(1/3) = 0.6667
    assert r.tier == "cascade"
    assert r.diagnosis_score == pytest.approx(CASCADE_BASE + 0.5 * (1 / 3), abs=1e-3)


def test_no_run_when_diagnosis_is_none() -> None:
    # No mock needed: None short-circuits before touching the ledger.
    r = score_run(None, since=_SINCE)
    assert r.tier == "no-run"
    assert r.diagnosis_score == 0.0
    assert not r.is_correct


def test_no_run_when_ledger_empty_in_window() -> None:
    r = _score(Diagnosis(failure_key="negative_price"), [])
    assert r.tier == "no-run"
