"""L2 END-TO-END inject -> detect -> score proof against the LIVE pipeline.

This is the probabilistic-verification capstone (R5): it closes the Sentinel loop
end to end against the REAL Component-A exhaust.

  inject (src.gen.engine, into the live Postgres source + I4 ledger)
    -> the crew's typed-Diagnosis seam (synthesize task / cascade Flow)
      -> score_run / score_diagnosis grading against the I4 oracle
        -> assert the score MATCHES ground truth.

WHAT IS LIVE vs WHAT IS STUBBED (the honesty contract):
  * LIVE: the inject (a real row written to the ``injected_incidents`` ledger in
    the running Postgres) and the scoring oracle (``sentinel.scoring`` reads that
    SAME ledger via ``src.gen.repository.session``). The cascade path additionally
    runs the REAL deterministic ``diagnose_cascade`` Flow against the live DuckDB
    warehouse. So the trigger/oracle wiring, the I4 ledger contract, the cascade
    routing, and the typed Diagnosis -> ScoreResult seam are all exercised for real.
  * STUBBED: the token-producing LLM call. The crew runs its REAL machinery
    (agent executor, ``output_pydantic=Diagnosis`` coercion exactly as
    ``sentinel.crew`` wires the synthesize task) with a :class:`StubLLM` scripted
    to emit the CORRECT ``failure_key`` for the case under test.

WHAT THIS PROVES (and what it does NOT): it proves the LOOP is wired correctly --
an injected incident becomes a ledger row, the crew's typed Diagnosis flows to the
oracle, and the oracle returns a MATCH against ground truth. It does NOT prove an
LLM is smart enough to diagnose the failure unaided. That -- the probabilistic
question of crew accuracy -- is the job of the live, API-key-gated scorecard in
``tests/sentinel/test_eval_live.py`` (skipped offline). Here the stub makes the
loop DETERMINISTIC and OFFLINE, so a green run means "the plumbing is correct",
never "the agent is correct" (HONESTY RULE / R5).

WHY THE SYNTHESIZE TASK, NOT A FULL HIERARCHICAL KICKOFF: on crewai 0.100.0 the
hierarchical manager loop needs precisely-shaped per-task delegation JSON to
terminate; a single generic scripted reply re-prompts forever (and a Diagnosis
reply fed to the postmortem task fails its converter -- verified). Driving the
synthesize task directly -- the agent + ``output_pydantic=Diagnosis`` exactly as
the crew wires it -- is the load-bearing crew->scoring seam and is deterministic.
Crew ASSEMBLY is covered by test_crew_build; the resolution squad + guardrail/HITL
unlocks by test_crew_stub; the cascade routing by test_flow. This module ties them
to the LIVE oracle.

REPRODUCIBILITY (R7): each case captures a ``since`` cursor from the DB clock and
reverts schema drift before injecting, so the scored window holds exactly that
case's ledger rows. The ``since`` cursor is the production reproducibility
primitive (the trigger's own), so isolating by it -- rather than truncating the
ledger -- exercises the real mechanism instead of a test-only shortcut.

SINGLE-WRITER DUCKDB: the cascade Flow is the only warehouse reader here; it opens
READ_ONLY and is serialised (one case at a time, no parallelism). The whole module
is skipped with a clear reason when Postgres is unreachable -- never a fabricated
scorecard.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

pytest.importorskip("sentinel.scoring", reason="Sentinel scoring not importable")

from crewai import Agent, Crew, Process, Task  # noqa: E402

from sentinel.models import Diagnosis  # noqa: E402
from sentinel.scoring import (  # noqa: E402
    ALIAS_SCORE,
    CASCADE_BASE,
    EXACT_SCORE,
    EXPECTED_SURFACE,
    score_diagnosis,
    score_run,
)
from src.gen import engine  # noqa: E402
from src.gen import repository as repo  # noqa: E402
from tests.sentinel.stub_llm import StubLLM  # noqa: E402

# --- Postgres availability gate (no live source -> skip, never fabricate) -----


def _postgres_reachable() -> bool:
    try:
        conn = repo.session()
        conn.close()
        return True
    except Exception:  # noqa: BLE001 - any connection failure means "no live source"
        return False


_PG_UP = _postgres_reachable()

pytestmark = pytest.mark.skipif(
    not _PG_UP,
    reason=(
        "e2e inject->detect->score needs the live Postgres source + I4 ledger "
        "(make up). Skipped, not faked, when the backbone is down (HONESTY RULE)."
    ),
)


# --- the 14 generator failures, split by how they are e2e-scorable -----------

# Single failures whose diagnosis is graded by EXACT failure_key match against the
# live I4 ledger. The StubLLM emits the correct key; the synthesize task coerces it
# into a typed Diagnosis exactly as the crew wires it; score_run reads the ledger.
# Every key in the registry except the cascade is here (the cascade is multi-row).
SINGLE_FAILURES = (
    "negative_price",
    "missing_customer",
    "invalid_quantity",
    "duplicate_order",
    "late_arrival",
    "volume_spike",
    "schema_drift",
    "orphan_payment",
    "recurring_incident",
    "ambiguous_anomaly",
    "destructive_fix",
    "malformed_data",
    "slow_source",
)

CASCADE_KEY = "multi_failure_cascade"
CASCADE_MEMBERS = ("missing_customer", "volume_spike", "schema_drift")


# --- helpers -----------------------------------------------------------------


def _db_now() -> datetime:
    """Read the DB clock for a since-cursor robust to host/DB clock skew."""
    conn = repo.session()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            return cur.fetchone()[0]
    finally:
        conn.close()


def _revert_schema_drift() -> None:
    """Restore a clean schema baseline (R7): undo a prior schema_drift rename so
    the next inject targets ``orders.customer_id``. Idempotent.
    """
    conn = repo.session()
    try:
        if repo.order_customer_column(conn) == "user_id":
            repo.execute(conn, "ALTER TABLE orders RENAME COLUMN user_id TO customer_id")
            conn.commit()
    finally:
        conn.close()


def _inject(failure_key: str) -> str:
    """Inject ONE failure into the live source + I4 ledger; return its detail."""
    with repo.session() as conn:
        result = engine.inject(conn, failure_key)
    return result.detail


def _ledger_window_keys(since: datetime) -> list[str]:
    """The failure_keys recorded at-or-after ``since`` (the scored window)."""
    conn = repo.session()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT failure_key FROM injected_incidents "
                "WHERE injected_at >= %s ORDER BY injected_at ASC, incident_id ASC",
                (since,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _final(payload: dict) -> str:
    """Wrap a payload as a CrewAI final answer so the agent executor terminates."""
    return "Thought: I have the diagnosis.\nFinal Answer: " + json.dumps(payload)


def _run_synthesize_with_stub(diagnosis_payload: dict) -> Diagnosis:
    """Drive the crew's synthesize seam with a StubLLM and return the typed output.

    This is the EXACT load-bearing wiring of ``SentinelCrew.synthesize_diagnosis_task``
    (an agent emitting ``output_pydantic=Diagnosis``), run offline with a scripted
    reply. It verifies the crew machinery, not LLM intelligence.
    """
    stub = StubLLM(default=_final(diagnosis_payload))
    agent = Agent(
        role="Data Profiler",
        goal="synthesize the diagnosis",
        backstory="grounds the verdict in I3 evidence",
        llm=stub,
        allow_delegation=False,
    )
    task = Task(
        description="Synthesize the incident diagnosis.",
        expected_output="A typed Diagnosis.",
        agent=agent,
        output_pydantic=Diagnosis,
    )
    crew = Crew(agents=[agent], tasks=[task], process=Process.sequential, verbose=False)
    out = crew.kickoff()
    assert isinstance(out.pydantic, Diagnosis), "synthesize task must emit a typed Diagnosis"
    return out.pydantic


# --- fixtures: reset-to-clean before each case (R7) ---------------------------


@pytest.fixture(autouse=True)
def _clean_baseline():
    """Revert schema drift before AND after each case so injects are reproducible
    and the schema is left clean for the rest of the suite (R7).
    """
    _revert_schema_drift()
    yield
    _revert_schema_drift()


# --- the e2e proof: one parametrized case per single failure -----------------


@pytest.mark.parametrize("failure_key", SINGLE_FAILURES)
def test_e2e_single_failure_scores_exact(failure_key: str) -> None:
    """inject -> the crew's typed Diagnosis seam (StubLLM = correct key) ->
    score against the LIVE I4 ledger -> assert an EXACT ground-truth match.

    Proves the loop is wired: the injected incident became a ledger row, the typed
    Diagnosis reached the oracle, and the oracle graded it correct. (The stub
    supplies the key; crew accuracy is the live eval's job -- see module docstring.)
    """
    since = _db_now()
    detail = _inject(failure_key)
    assert failure_key in _ledger_window_keys(since), f"inject did not record {failure_key} in I4"

    payload = {
        "failure_key": failure_key,
        "sub_failures": [],
        # Cite the canonical surface so the (non-gating) evidence tier is exercised
        # too; falls back to empty for A2-only failures with no I3 surface.
        "evidence_surface": EXPECTED_SURFACE.get(failure_key, ""),
        "confidence": 0.9,
        "summary": f"{failure_key}: {detail}",
    }
    diagnosis = _run_synthesize_with_stub(payload)
    assert diagnosis.failure_key == failure_key

    score = score_run(diagnosis, since=since)

    # Tier-1 (gating): an EXACT match against the I4 oracle.
    assert score.tier == "exact", f"{failure_key}: expected exact, got {score.tier} ({score.detail})"
    assert score.diagnosis_score == EXACT_SCORE
    assert score.matched == failure_key
    assert failure_key in score.oracle_keys

    # The legacy string API agrees (back-compat oracle path).
    assert score_diagnosis(failure_key) == "correct"


def test_e2e_recurring_incident_alias_scores() -> None:
    """The recurring_incident injector writes negative_price rows, so a crew that
    says "negative_price" for a recurring incident is substantively correct: the
    oracle's registry-justified ALIAS tier (0.7) must fire against the live ledger.
    """
    since = _db_now()
    _inject("recurring_incident")
    assert "recurring_incident" in _ledger_window_keys(since)

    # Diagnose the SUBSTANCE (negative prices) rather than the meta-pattern key.
    payload = {
        "failure_key": "negative_price",
        "sub_failures": [],
        "evidence_surface": EXPECTED_SURFACE["negative_price"],
        "confidence": 0.8,
        "summary": "recurring negative-price rows quarantined",
    }
    diagnosis = _run_synthesize_with_stub(payload)
    score = score_run(diagnosis, since=since)

    assert score.tier == "alias", f"expected alias, got {score.tier} ({score.detail})"
    assert score.diagnosis_score == ALIAS_SCORE
    assert score.matched == "recurring_incident"


def test_e2e_miss_is_scored_honestly_not_hidden() -> None:
    """Negative control: a WRONG diagnosis against a real injected incident must
    score a MISS (0.0), proving the oracle actually grades and is not rubber-
    stamping the stub's reply (HONESTY RULE).
    """
    since = _db_now()
    _inject("negative_price")

    payload = {
        "failure_key": "orphan_payment",  # deliberately wrong
        "sub_failures": [],
        "evidence_surface": "",
        "confidence": 0.5,
        "summary": "wrong on purpose",
    }
    diagnosis = _run_synthesize_with_stub(payload)
    score = score_run(diagnosis, since=since)

    assert score.tier == "miss"
    assert score.diagnosis_score == 0.0
    assert score.matched == "negative_price"  # graded against the TRUE oracle key


# --- the cascade: real deterministic Flow against the LIVE pipeline -----------


def test_e2e_cascade_scores_against_live_ledger() -> None:
    """multi_failure_cascade: inject the 3-member cascade into the live source/
    ledger, run the REAL deterministic ``diagnose_cascade`` Flow against the live
    warehouse, and score against the per-member oracle rows.

    Honest, frozen-warehouse-tolerant assertion: the warehouse is a clean snapshot,
    so freshly injected Postgres defects have not flowed through dbt yet -- the Flow
    detects only what the current silver state actually shows. We therefore assert
    the STRUCTURAL ground truth that holds regardless of warehouse freshness (this
    is the cascade ROUTING + oracle wiring being proven), and report the member
    overlap rather than demanding a brittle full-overlap that needs a 173k dbt run.
    Full-overlap scoring is proven deterministically in the fixture-tool case below.
    """
    from sentinel.flow import diagnose_cascade

    since = _db_now()
    _inject(CASCADE_KEY)

    window = _ledger_window_keys(since)
    # The injector records one row per member (and a top-level cascade row).
    for member in CASCADE_MEMBERS:
        assert member in window, f"cascade member {member} missing from I4 window"

    diagnosis = diagnose_cascade()  # real Flow, live read-only warehouse read
    assert diagnosis.failure_key == CASCADE_KEY

    score = score_run(diagnosis, since=since)
    assert score.tier == "cascade", f"expected cascade tier, got {score.tier}"
    assert score.matched == CASCADE_KEY
    # Base credit always; member overlap depends on warehouse freshness (reported).
    assert score.diagnosis_score >= CASCADE_BASE
    # The oracle compared against the real member set, not a guess.
    assert set(CASCADE_MEMBERS).issubset(set(score.oracle_keys))
    print(
        f"[E2E-CASCADE] live: tier={score.tier} score={score.diagnosis_score} "
        f"named={sorted(diagnosis.sub_failures)} detail={score.detail!r}"
    )


def test_e2e_cascade_full_overlap_is_deterministic() -> None:
    """Same live ledger, but the cascade Flow is given fixture tools that report
    ALL three members found -- proving the cascade scores a FULL-overlap 1.0
    deterministically (base 0.5 + 0.5 * 3/3) without a heavy dbt run. This isolates
    the scorer's cascade tier from warehouse freshness while still grading against
    the live per-member oracle rows.
    """
    from sentinel.flow import diagnose_cascade

    class _AllFoundProfile:
        def _run(self, failure_key: str, **_: object) -> str:
            return json.dumps({"found": failure_key in CASCADE_MEMBERS})

    class _DbtFound:
        def _run(self, **_: object) -> str:
            return json.dumps({"found": True})

    since = _db_now()
    _inject(CASCADE_KEY)

    diagnosis = diagnose_cascade(profile_tool=_AllFoundProfile(), dbt_tool=_DbtFound())
    assert set(diagnosis.sub_failures) == set(CASCADE_MEMBERS)

    score = score_run(diagnosis, since=since)
    assert score.tier == "cascade"
    assert score.diagnosis_score == pytest.approx(1.0)


# --- coverage guard: every registry failure is accounted for in this loop -----


def test_every_registry_failure_is_covered() -> None:
    """Meta-test: the union of the single-failure params and the cascade key must
    equal the full generator registry, so a newly added failure cannot silently
    escape the e2e scorecard (R6: the crew must handle all 14).
    """
    from src.gen.failures import REGISTRY

    covered = set(SINGLE_FAILURES) | {CASCADE_KEY}
    assert covered == set(REGISTRY), (
        "e2e coverage drift: "
        f"missing={set(REGISTRY) - covered} extra={covered - set(REGISTRY)}"
    )
