"""L1 unit tests for the B1 trigger (no LLM, no live DB).

The trigger is the inject -> detect -> score entrypoint. Everything here runs
against a FIXTURE I4 ledger (a mocked psycopg connection) and an injectable crew
factory, so the full poll/dispatch/score loop is verifiable without a live
database or API key. The cascade path is deterministic (the Flow reads evidence,
not an LLM) and is exercised with mocked tools.

Verified:
  * poll_once returns only incidents at-or-after the cursor, oldest first, and
    advances the cursor to the newest injected_at (or stays put when nothing new).
  * dispatch routes a single incident to the base crew and a multi-member window
    to the cascade Flow.
  * a destructive incident builds the crew human_gated_fix=True (HITL unlock).
  * a crew that cannot run (no key / crash) yields an honest no-run, never a fake
    score (HONESTY RULE).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("sentinel.trigger", reason="Sentinel trigger not importable")

from sentinel.models import Diagnosis  # noqa: E402
from sentinel.trigger import (  # noqa: E402
    Incident,
    dispatch,
    poll_once,
    run_sentinel,
)

_SINCE = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)


def _ledger_conn(rows: list[tuple]) -> MagicMock:
    """Mock a psycopg connection whose cursor.fetchall returns ledger rows.

    Each row is (incident_id, failure_key, detail, detected_by, injected_at) —
    the injected_incidents shape the trigger SELECTs.
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _factory(rows: list[tuple]):
    return lambda: _ledger_conn(rows)


# --- poll_once ---------------------------------------------------------------
def test_poll_once_returns_incidents_and_advances_cursor() -> None:
    t1 = _SINCE + timedelta(seconds=10)
    t2 = _SINCE + timedelta(seconds=20)
    rows = [
        (1, "negative_price", "neg price", "Data Profiler", t1),
        (2, "missing_customer", "null cust", "Data Profiler", t2),
    ]
    result = poll_once(_SINCE, session_factory=_factory(rows))
    assert [i.failure_key for i in result.incidents] == ["negative_price", "missing_customer"]
    assert result.cursor == t2  # advanced to newest
    assert result.since == _SINCE


def test_poll_once_empty_window_keeps_cursor() -> None:
    result = poll_once(_SINCE, session_factory=_factory([]))
    assert result.incidents == []
    assert result.cursor == _SINCE  # no new rows -> cursor unchanged


# --- dispatch: base path -----------------------------------------------------
class _StubCrew:
    """A crew-factory stand-in that returns a scripted Diagnosis on kickoff.

    Records the human_gated_fix flag it was constructed with so the HITL routing
    can be asserted without a live LLM.
    """

    last_human_gated: bool | None = None

    def __init__(self, *, human_gated_fix: bool = False) -> None:
        type(self).last_human_gated = human_gated_fix
        self._human_gated = human_gated_fix

    def crew(self) -> _StubCrew:
        return self

    def kickoff(self, inputs=None):  # noqa: ANN001 - mirrors Crew.kickoff
        key = (inputs or {}).get("failure_key", "negative_price")
        out = MagicMock()
        out.pydantic = Diagnosis(failure_key=key, evidence_surface="silver_orders_rejects")
        out.tasks_output = []
        return out


def _single_incident(key: str) -> Incident:
    return Incident(
        incident_id=1, failure_key=key, detail="x", detected_by="Data Profiler", injected_at=_SINCE
    )


def test_dispatch_single_incident_routes_to_base_crew() -> None:
    inc = _single_incident("negative_price")
    with patch(
        "sentinel.trigger.score_run",
        return_value=_score_stub("exact", 1.0),
    ):
        result = dispatch([inc], _SINCE, crew_factory=_StubCrew)
    assert result.route == "base"
    assert result.diagnosis is not None
    assert result.diagnosis.failure_key == "negative_price"
    assert result.human_gated is False


def test_dispatch_destructive_incident_is_human_gated() -> None:
    inc = _single_incident("destructive_fix")
    with patch("sentinel.trigger.score_run", return_value=_score_stub("exact", 1.0)):
        result = dispatch([inc], _SINCE, crew_factory=_StubCrew)
    assert result.human_gated is True  # HITL unlock: A4 pauses for approval
    assert _StubCrew.last_human_gated is True


def test_dispatch_no_incidents_is_no_run() -> None:
    with patch("sentinel.trigger.score_run", return_value=_score_stub("no-run", 0.0)):
        result = dispatch([], _SINCE)
    assert result.route == "no-run"
    assert result.diagnosis is None


def test_dispatch_crew_crash_is_honest_no_run() -> None:
    # No API key / crew crash -> NO-RUN with the typed diagnosis None, never a fake
    # score. The crew factory raises (as a keyless kickoff would).
    class _Boom:
        def __init__(self, *, human_gated_fix: bool = False) -> None:
            pass

        def crew(self):  # noqa: ANN202
            raise RuntimeError("no OpenAI API key")

    inc = _single_incident("negative_price")
    with patch("sentinel.trigger.score_run", return_value=_score_stub("no-run", 0.0)):
        result = dispatch([inc], _SINCE, crew_factory=_Boom)
    assert result.route == "no-run"
    assert result.diagnosis is None
    assert "no OpenAI API key" in result.detail


# --- dispatch: cascade path (deterministic Flow, no LLM) ---------------------
def test_dispatch_multi_member_window_routes_to_cascade() -> None:
    members = ["missing_customer", "volume_spike", "schema_drift"]
    incidents = [
        Incident(i + 1, k, "x", "Data Profiler", _SINCE + timedelta(seconds=i))
        for i, k in enumerate(members)
    ]
    cascade_dx = Diagnosis(failure_key="multi_failure_cascade", sub_failures=members)
    with (
        patch("sentinel.flow.diagnose_cascade", return_value=cascade_dx),
        patch("sentinel.trigger.score_run", return_value=_score_stub("cascade", 1.0)),
    ):
        result = dispatch(incidents, _SINCE)
    assert result.route == "cascade"
    assert set(result.failure_keys) == set(members)
    assert result.diagnosis.failure_key == "multi_failure_cascade"


# --- run_sentinel: end-to-end poll + dispatch over the fixture ledger --------
def test_run_sentinel_polls_then_dispatches_base() -> None:
    rows = [(1, "negative_price", "neg", "Data Profiler", _SINCE + timedelta(seconds=5))]
    with patch("sentinel.trigger.score_run", return_value=_score_stub("exact", 1.0)):
        result = run_sentinel(
            since=_SINCE,
            session_factory=_factory(rows),
            crew_factory=_StubCrew,
        )
    assert result.route == "base"
    assert result.failure_keys == ["negative_price"]


def test_run_sentinel_none_since_reports_no_run() -> None:
    # since=None captures "now"; an empty fixture ledger -> honest no-run.
    with patch("sentinel.trigger.score_run", return_value=_score_stub("no-run", 0.0)):
        result = run_sentinel(session_factory=_factory([]), crew_factory=_StubCrew)
    assert result.route == "no-run"


# --- helpers -----------------------------------------------------------------
def _score_stub(tier: str, diagnosis_score: float):
    from sentinel.scoring import ScoreResult

    return ScoreResult(
        diagnosis_score=diagnosis_score,
        evidence_score=0.0,
        tier=tier,
        matched="",
        detail=f"stub {tier}",
    )
