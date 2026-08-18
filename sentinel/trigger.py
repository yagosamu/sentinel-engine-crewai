"""B1 trigger — the Sentinel's entrypoint into Component B.

The trigger POLLS the ``injected_incidents`` ledger (I4) rather than hooking a
Dagster failure webhook. The decisive reason (architect design, confidence 0.88):
only ~2 of the 14 failures actually CRASH a Dagster run — the other ~12 are
QUARANTINED rows in ``silver_*_rejects`` (the backbone run SUCCEEDS, that is the
whole point of quarantine-not-drop), so a failure-webhook would never fire for
them. Polling the SAME table the scorer reads (I4) keeps the trigger and the
oracle consistent: every injected incident is a trigger AND a scorable event, and
the loop is a pure function of ledger state (trivially reproducible in tests with
a fixed ``since`` cursor — R7).

R3 (one-way dependency): the trigger reads I4 read-only via
``src.gen.repository.session()`` and writes NOTHING to Postgres or ``platform/``.
The backbone never pushes to B; B pulls.

DISPATCH:
  * A single incident in the window -> the base hierarchical crew
    (:class:`~sentinel.crew.SentinelCrew`). Its synthesize task emits a typed
    :class:`~sentinel.models.Diagnosis`.
  * MULTIPLE distinct failure_keys in the window -> a ``multi_failure_cascade``,
    routed to the deterministic :func:`~sentinel.flow.diagnose_cascade` Flow. (The
    cascade injector writes one ledger row per member — missing_customer +
    volume_spike + schema_drift — and NO top-level cascade row, so the trigger
    recognises a cascade by the simultaneous arrival of several distinct members.)

HONESTY (R5 / no-API-key): running the base crew requires an LLM. When no key is
present the crew kickoff raises; :func:`run_sentinel` captures that and records a
NO-RUN dispatch (``diagnosis=None``), which :func:`~sentinel.scoring.score_run`
grades as ``tier="no-run"`` — never a fabricated score. The deterministic cascade
Flow needs no key, so a cascade is always scorable offline.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sentinel.models import Diagnosis
from sentinel.scoring import ScoreResult, score_run
from src.gen.repository import session

# Failures whose remediation is destructive — when one is dispatched the crew is
# built ``human_gated_fix=True`` so A4's propose-fix task pauses for approval (the
# destructive_fix HITL unlock). Mirror of crew.HUMAN_GATED_FAILURES.
HUMAN_GATED_FAILURES: frozenset[str] = frozenset({"destructive_fix"})

CASCADE_KEY = "multi_failure_cascade"


@dataclass(frozen=True, slots=True)
class Incident:
    """One row of the I4 ledger (the trigger's unit of work). Read-only mirror of
    the ``injected_incidents`` shape; the trigger never writes it back.
    """

    incident_id: int
    failure_key: str
    detail: str
    detected_by: str
    injected_at: datetime


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """The outcome of dispatching one ledger window to the crew/flow + its score.

    ``route`` is ``"base"`` (single failure -> hierarchical crew),
    ``"cascade"`` (several members -> Flow), or ``"no-run"`` (crew could not run,
    e.g. no API key). ``diagnosis`` is the typed crew verdict (or ``None`` on a
    no-run). ``score`` is graded against the I4 oracle — REPORTED, not asserted.
    """

    route: str
    failure_keys: list[str]
    diagnosis: Diagnosis | None
    score: ScoreResult
    since: datetime
    detail: str = ""
    human_gated: bool = False


@dataclass(frozen=True, slots=True)
class PollResult:
    """A poll over the ledger: the new incidents and the advanced cursor.

    ``cursor`` is the timestamp callers pass back as the next ``since`` (the most
    recent ``injected_at`` seen, or the input ``since`` when nothing was new). It
    is the load-bearing reproducibility primitive (also feeds cascade scoring).
    """

    incidents: list[Incident]
    cursor: datetime
    since: datetime


def poll_once(since: datetime, *, session_factory: Callable[[], Any] = session) -> PollResult:
    """Read all I4 incidents injected at-or-after ``since`` (read-only, pure).

    Args:
        since: The run-window start. Incidents with ``injected_at >= since`` are
            returned, oldest first (so the window is ordered for cascade grading).
        session_factory: Injectable connection factory (defaults to the live I4
            ledger via ``repository.session``); tests pass a fixture.

    Returns:
        A :class:`PollResult` with the new incidents and the advanced cursor.
    """
    conn = session_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT incident_id, failure_key, detail, detected_by, injected_at "
                "FROM injected_incidents WHERE injected_at >= %s "
                "ORDER BY injected_at ASC, incident_id ASC",
                (since,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    incidents = [
        Incident(
            incident_id=r[0],
            failure_key=r[1],
            detail=r[2],
            detected_by=r[3],
            injected_at=r[4],
        )
        for r in rows
    ]
    cursor = incidents[-1].injected_at if incidents else since
    return PollResult(incidents=incidents, cursor=cursor, since=since)


def _is_cascade(incidents: list[Incident]) -> bool:
    """A cascade is several DISTINCT member failures in one window (the cascade
    injector writes one row per member and no top-level cascade row), or an
    explicit ``multi_failure_cascade`` row if one is ever recorded.
    """
    keys = {i.failure_key for i in incidents}
    if CASCADE_KEY in keys:
        return True
    return len(keys) > 1


def _dispatch_cascade(incidents: list[Incident], since: datetime) -> DispatchResult:
    """Route a multi-member window to the deterministic cascade Flow (offline-safe).

    The Flow reads the REAL evidence surfaces (I2/I3) read-only and emits a typed
    Diagnosis carrying ``failure_key="multi_failure_cascade"`` + the detected
    members; the score is graded against the per-member oracle rows in the window.
    """
    from sentinel.flow import diagnose_cascade

    diagnosis = diagnose_cascade()
    score = score_run(diagnosis, since=since)
    keys = sorted({i.failure_key for i in incidents})
    return DispatchResult(
        route="cascade",
        failure_keys=keys,
        diagnosis=diagnosis,
        score=score,
        since=since,
        detail=f"cascade: members {keys} routed to SentinelFlow",
    )


def _dispatch_base(
    incident: Incident,
    since: datetime,
    *,
    crew_factory: Callable[..., Any] | None = None,
) -> DispatchResult:
    """Route a single incident to the base hierarchical crew, then score it.

    Args:
        crew_factory: Injectable ``SentinelCrew``-like factory (tests pass a stub
            so no live LLM is called). It is invoked with ``human_gated_fix=...``
            so a destructive incident pauses A4's propose task (HITL unlock); its
            ``.crew()`` is kicked off and the typed Diagnosis is extracted.
    """
    human_gated = incident.failure_key in HUMAN_GATED_FAILURES

    if crew_factory is None:
        from sentinel.crew import SentinelCrew

        crew_factory = SentinelCrew

    diagnosis: Diagnosis | None = None
    detail = ""
    try:
        sentinel = crew_factory(human_gated_fix=human_gated)
        output = sentinel.crew().kickoff(inputs={"failure_key": incident.failure_key})
        diagnosis = _extract_diagnosis(output)
        if diagnosis is None:
            detail = "crew completed but produced no typed Diagnosis"
    except Exception as exc:  # noqa: BLE001 - any kickoff failure -> honest no-run
        # No API key, embedder error, or a crew crash -> NO-RUN, never a fake score.
        detail = f"crew did not run: {type(exc).__name__}: {exc}"

    score = score_run(diagnosis, since=since)
    route = "base" if diagnosis is not None else "no-run"
    return DispatchResult(
        route=route,
        failure_keys=[incident.failure_key],
        diagnosis=diagnosis,
        score=score,
        since=since,
        detail=detail,
        human_gated=human_gated,
    )


def _extract_diagnosis(output: Any) -> Diagnosis | None:
    """Pull the typed :class:`Diagnosis` out of a CrewAI kickoff result.

    The synthesize task carries ``output_pydantic=Diagnosis``; CrewAI exposes the
    last task's typed output on ``CrewOutput.pydantic``. Falls back to the
    per-task ``tasks_output`` and finally returns ``None`` (a no-run) rather than
    fabricating a diagnosis.
    """
    if output is None:
        return None
    pydantic = getattr(output, "pydantic", None)
    if isinstance(pydantic, Diagnosis):
        return pydantic
    for task_out in reversed(getattr(output, "tasks_output", []) or []):
        candidate = getattr(task_out, "pydantic", None)
        if isinstance(candidate, Diagnosis):
            return candidate
    return None


def dispatch(
    incidents: list[Incident],
    since: datetime,
    *,
    crew_factory: Callable[..., Any] | None = None,
) -> DispatchResult:
    """Route a polled window to the right path and score the result.

    Several distinct members -> the deterministic cascade Flow; a single member ->
    the base hierarchical crew. An empty window is a no-run.
    """
    if not incidents:
        return DispatchResult(
            route="no-run",
            failure_keys=[],
            diagnosis=None,
            score=score_run(None, since=since),
            since=since,
            detail="no new incident in the polled window",
        )
    if _is_cascade(incidents):
        return _dispatch_cascade(incidents, since)
    return _dispatch_base(incidents[-1], since, crew_factory=crew_factory)


def run_sentinel(
    since: datetime | None = None,
    *,
    session_factory: Callable[[], Any] = session,
    crew_factory: Callable[..., Any] | None = None,
) -> DispatchResult:
    """B1 entrypoint: poll I4 since the cursor, dispatch the window, score it.

    Args:
        since: The run-window start. ``None`` captures "now" and reports a no-run
            (nothing has been injected since this instant) — callers normally pass
            a cursor captured just before ``make inject`` so the inject -> detect
            -> score loop is reproducible (R7).
        session_factory / crew_factory: Injectable seams for tests (a fixture
            ledger and a stub-LLM crew), so the full loop is verifiable without a
            live database or API key.

    Returns:
        A :class:`DispatchResult`. The score is REPORTED against the I4 oracle,
        never asserted correct.
    """
    if since is None:
        since = datetime.now(UTC)
    poll = poll_once(since, session_factory=session_factory)
    return dispatch(poll.incidents, since, crew_factory=crew_factory)


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m sentinel.trigger [--since-now]`` — poll the live ledger and
    print the dispatch + score. Reads I4 read-only; writes nothing.
    """
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Sentinel B1 trigger (poll I4, dispatch, score).")
    parser.add_argument(
        "--since",
        default=None,
        help=(
            "ISO timestamp window start; default = now (reports a no-run unless "
            "you pass a cursor captured before inject)."
        ),
    )
    args = parser.parse_args(argv)
    since = datetime.fromisoformat(args.since) if args.since else datetime.now(UTC)

    result = run_sentinel(since)
    print(
        json.dumps(
            {
                "route": result.route,
                "failure_keys": result.failure_keys,
                "human_gated": result.human_gated,
                "diagnosis_key": result.diagnosis.failure_key if result.diagnosis else None,
                "score": {
                    "tier": result.score.tier,
                    "diagnosis_score": result.score.diagnosis_score,
                    "evidence_score": result.score.evidence_score,
                    "matched": result.score.matched,
                    "detail": result.score.detail,
                },
                "detail": result.detail,
                "since": since.isoformat(),
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
