"""L2 wiring tests for the Sentinel crew (no live LLM).

These assert the crew ASSEMBLES correctly — hierarchical process, a manager_agent
that is excluded from the specialist roster, the A2/A3 squad with their read-only
tools, and the synthesize task carrying ``output_pydantic=Diagnosis``. They do NOT
run a live kickoff (no API key) and make no claim about diagnostic correctness.
"""

from __future__ import annotations

import pytest

pytest.importorskip("sentinel.crew", reason="Sentinel crew not importable")

from crewai import Process  # noqa: E402

from sentinel.crew import SentinelCrew  # noqa: E402
from sentinel.models import Diagnosis, Postmortem  # noqa: E402
from tests.sentinel.stub_llm import StubLLM  # noqa: E402


def test_crew_builds_hierarchical() -> None:
    crew = SentinelCrew().crew()
    assert crew.process == Process.hierarchical


def test_manager_agent_present_and_delegating() -> None:
    crew = SentinelCrew().crew()
    assert crew.manager_agent is not None
    assert crew.manager_agent.role == "Sentinel Tech Lead"
    assert crew.manager_agent.allow_delegation is True


def test_manager_excluded_from_specialist_roster() -> None:
    # Hierarchical contract: the manager_agent must NOT appear in agents=[].
    # The full crew is the investigation squad (A2/A3) + the resolution squad
    # (A4 Data Engineer, A5 Incident Commander).
    crew = SentinelCrew().crew()
    roles = {a.role for a in crew.agents}
    assert crew.manager_agent.role not in roles
    assert roles == {
        "Sentinel Log Analyst",
        "Sentinel Data Profiler",
        "Sentinel Data Engineer",
        "Sentinel Incident Commander",
    }


def test_specialists_do_not_delegate() -> None:
    crew = SentinelCrew().crew()
    assert all(a.allow_delegation is False for a in crew.agents)


def test_investigation_tools_are_wired() -> None:
    crew = SentinelCrew().crew()
    by_role = {a.role: a for a in crew.agents}
    log_tools = {t.name for t in by_role["Sentinel Log Analyst"].tools}
    profiler_tools = {t.name for t in by_role["Sentinel Data Profiler"].tools}
    assert log_tools == {"read_dagster_logs", "read_dbt_run_results"}
    assert profiler_tools == {"profile_rejects", "query_duckdb"}


def test_resolution_tools_are_wired() -> None:
    # A4 Data Engineer proposes (gated) patches; A5 Incident Commander recalls
    # prior incidents (I5 RAG) and writes the typed post-mortem.
    crew = SentinelCrew().crew()
    by_role = {a.role: a for a in crew.agents}
    engineer_tools = {t.name for t in by_role["Sentinel Data Engineer"].tools}
    commander_tools = {t.name for t in by_role["Sentinel Incident Commander"].tools}
    assert engineer_tools == {"propose_patch"}
    assert commander_tools == {"query_incident_rag", "write_postmortem"}


def test_log_analyst_has_retry_for_slow_source() -> None:
    crew = SentinelCrew().crew()
    log_analyst = next(a for a in crew.agents if a.role == "Sentinel Log Analyst")
    assert (log_analyst.max_retry_limit or 0) >= 1


def test_synthesize_task_emits_typed_diagnosis() -> None:
    crew = SentinelCrew().crew()
    pydantic_tasks = [t for t in crew.tasks if t.output_pydantic is Diagnosis]
    assert len(pydantic_tasks) == 1


def test_postmortem_task_is_typed_and_guardrailed() -> None:
    # malformed_data unlock: the post-mortem task carries output_pydantic +
    # a validation guardrail + bounded retries (typed report even from garbage).
    crew = SentinelCrew().crew()
    pm_tasks = [t for t in crew.tasks if t.output_pydantic is Postmortem]
    assert len(pm_tasks) == 1
    pm = pm_tasks[0]
    assert pm.guardrail is not None
    assert (pm.max_retries or 0) >= 1


def test_human_gate_off_by_default_on_under_for_destructive() -> None:
    # destructive_fix HITL unlock: the propose-fix task only pauses for human
    # approval when the crew is built human_gated_fix=True (per incident).
    base = SentinelCrew().crew()
    propose_base = next(t for t in base.tasks if "propose" in (t.description or "").lower())
    assert propose_base.human_input is False

    gated = SentinelCrew(human_gated_fix=True).crew()
    propose_gated = next(t for t in gated.tasks if "propose" in (t.description or "").lower())
    assert propose_gated.human_input is True


def test_memory_unlock_is_off_by_default() -> None:
    # recurring_incident unlock: crew memory is a crew capability, OFF by default
    # so the crew builds offline (memory=True eagerly constructs an embedder, which
    # needs an API key — see test_memory_unlock_requires_embedder).
    assert SentinelCrew().crew().memory is False


def test_memory_unlock_requires_embedder() -> None:
    # The flag IS plumbed through to Crew(memory=...): requesting it without a key
    # fails at the embedder, proving it is not silently dropped (CrewAI 0.100.0
    # builds the memory embedder at construction time). This is why memory is
    # opt-in and only exercised in the live (keyed) eval.
    with pytest.raises(Exception, match="(?i)api key|openai|embed"):
        SentinelCrew(memory=True).crew()


def test_stub_llm_is_applied_to_every_agent() -> None:
    # The stub-LLM seam: passing llm_override stamps the stub on manager + squad,
    # so a wiring test can run without a live API key.
    stub = StubLLM(default="ACK")
    crew = SentinelCrew(llm_override=stub).crew()
    assert crew.manager_agent.llm is stub
    assert all(a.llm is stub for a in crew.agents)


def test_task_count_is_the_full_remediation_chain() -> None:
    # Investigation (analyze logs -> profile warehouse -> synthesize) then
    # resolution (propose gated fix -> write typed post-mortem).
    crew = SentinelCrew().crew()
    assert len(crew.tasks) == 5
