"""L2 stub-LLM wiring tests for the resolution squad + advanced-feature unlocks.

These run the REAL CrewAI task machinery (agent executor, output_pydantic coercion,
the guardrail retry loop, human_input gating) with a SCRIPTED ``crewai.LLM`` stub
in place of the token-producing call — so the wiring is verified WITHOUT a live API
key and WITHOUT claiming the LLM diagnosed anything (HONESTY RULE / R5).

WHY SEQUENTIAL SINGLE/SMALL TASKS, NOT A FULL HIERARCHICAL KICKOFF: the
hierarchical manager loop needs carefully-shaped delegation tool-call JSON to
terminate; a naive scripted reply makes the executor re-prompt forever. Driving the
RESOLUTION tasks directly (the agent + task + guardrail + output_pydantic exactly as
the crew wires them) verifies the load-bearing machinery deterministically. The
crew ASSEMBLY (manager, roster, tools, task chain) is covered by test_crew_build;
the cascade Flow end-to-end is covered by test_trigger / test_flow.

The stub returns the CrewAI "Final Answer:" marker so the agent executor accepts the
reply as final and the loop terminates in one call.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("sentinel.crew", reason="Sentinel crew not importable")

from crewai import Agent, Crew, Process, Task  # noqa: E402

from sentinel.guardrails import validate_postmortem  # noqa: E402
from sentinel.models import Diagnosis, Postmortem  # noqa: E402
from tests.sentinel.stub_llm import StubLLM  # noqa: E402


def _final(payload: dict | str) -> str:
    """Wrap a payload as a CrewAI final answer so the agent executor terminates."""
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return f"Thought: I have the answer.\nFinal Answer: {body}"


def _run_single(task: Task, agent: Agent) -> object:
    crew = Crew(agents=[agent], tasks=[task], process=Process.sequential, verbose=False)
    return crew.kickoff()


# --- output_pydantic: the synthesize -> scoring typed seam --------------------
def test_synthesize_emits_typed_diagnosis_via_stub() -> None:
    diag = {
        "failure_key": "negative_price",
        "sub_failures": [],
        "evidence_surface": "silver_orders_rejects",
        "confidence": 0.9,
        "summary": "negative unit_price rows quarantined",
    }
    stub = StubLLM(default=_final(diag))
    agent = Agent(role="Profiler", goal="diagnose", backstory="b", llm=stub, allow_delegation=False)
    task = Task(
        description="Emit a Diagnosis.",
        expected_output="A typed Diagnosis.",
        agent=agent,
        output_pydantic=Diagnosis,
    )
    out = _run_single(task, agent)
    assert isinstance(out.pydantic, Diagnosis)
    assert out.pydantic.failure_key == "negative_price"
    assert out.pydantic.evidence_surface == "silver_orders_rejects"


# --- Guardrails + output_pydantic: the malformed_data unlock -----------------
def test_postmortem_guardrail_accepts_clean_typed_report() -> None:
    pm = {
        "incident_key": "malformed_data",
        "root_cause": "source rows carried non-printable noise; quarantined in rejects",
        "evidence": "silver_orders_rejects",
        "proposed_fix": "add a charset guard in bronze ingestion",
        "recurrence_note": "first observed",
    }
    stub = StubLLM(default=_final(pm))
    agent = Agent(role="Commander", goal="write postmortem", backstory="b", llm=stub, allow_delegation=False)
    task = Task(
        description="Write the post-mortem.",
        expected_output="A typed Postmortem.",
        agent=agent,
        output_pydantic=Postmortem,
        guardrail=validate_postmortem,
        max_retries=2,
    )
    out = _run_single(task, agent)
    assert isinstance(out.pydantic, Postmortem)
    assert out.pydantic.incident_key == "malformed_data"


def test_postmortem_guardrail_rejects_garbage_then_accepts_retry() -> None:
    # malformed_data unlock in action: the first reply smuggles raw garbage into a
    # structured field (the guardrail rejects it), the retry summarises (accepted).
    garbage = {
        "incident_key": "malformed_data",
        "root_cause": "raw dump follows: <script>alert(1)</script> 0x00 garbage bytes",
        "evidence": "silver_orders_rejects",
        "proposed_fix": "n/a",
        "recurrence_note": "n/a",
    }
    clean = {
        "incident_key": "malformed_data",
        "root_cause": "malformed UTF-8 / control bytes in source rows; quarantined in rejects",
        "evidence": "silver_orders_rejects",
        "proposed_fix": "add a charset guard in bronze ingestion",
        "recurrence_note": "first observed",
    }
    stub = StubLLM(replies=[_final(garbage), _final(clean)])
    agent = Agent(role="Commander", goal="write postmortem", backstory="b", llm=stub, allow_delegation=False)
    task = Task(
        description="Write the post-mortem.",
        expected_output="A typed Postmortem.",
        agent=agent,
        output_pydantic=Postmortem,
        guardrail=validate_postmortem,
        max_retries=2,
    )
    out = _run_single(task, agent)
    assert isinstance(out.pydantic, Postmortem)
    # The retry's clean root cause survived; the garbage one did not.
    assert "<script>" not in out.pydantic.root_cause
    assert "charset guard" in out.pydantic.proposed_fix
    # Proves the guardrail fired at least once (>=2 LLM calls: reject -> retry).
    assert len(stub.calls) >= 2


# --- HITL: the destructive_fix unlock (task-level, no interactive prompt) -----
def test_propose_fix_task_is_human_gated_for_destructive() -> None:
    from sentinel.crew import SentinelCrew

    gated = SentinelCrew(human_gated_fix=True).crew()
    propose = next(t for t in gated.tasks if "propose" in (t.description or "").lower())
    assert propose.human_input is True

    ungated = SentinelCrew(human_gated_fix=False).crew()
    propose2 = next(t for t in ungated.tasks if "propose" in (t.description or "").lower())
    assert propose2.human_input is False
