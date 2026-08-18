"""The Sentinel hierarchical crew (Component B, Layer 6).

A1 Manager (manager_agent) delegates to two squads:

  * Investigation — A2 Log Analyst (I1/I2) + A3 Data Profiler (I3). The
    synthesize task emits a typed :class:`~sentinel.models.Diagnosis` that
    :mod:`sentinel.scoring` grades against the I4 ledger.
  * Resolution — A4 Data Engineer (a GATED proposed patch, never applied to
    platform/) + A5 Incident Commander (I5 RAG recall + a typed, guardrail-
    validated post-mortem).

HIERARCHICAL MANAGER CONTRACT (CrewAI 0.100.0, verified against the installed
package): with a custom ``manager_agent`` you pass ``manager_agent=`` and do NOT
also pass ``manager_llm``; the manager must NOT appear in ``agents=[...]`` (CrewAI
injects it) and must have ``allow_delegation=True``. The specialists go in
``agents=[]`` with ``allow_delegation=False`` (only the manager delegates).

ADVANCED-FEATURE UNLOCKS wired here (the 14-failure capability map):
  * Memory (recurring_incident)  -> Crew(memory=True). Agent has no `memory`
    field in 0.100.0; memory is a crew capability. Opt-in via memory= because it
    needs an embedder (an API key) at runtime.
  * Knowledge/RAG (ambiguous_anomaly) -> knowledge_sources from the runbook.
    Opt-in (with_knowledge=) because attaching a knowledge source triggers
    embedding, which needs a key — the default build stays offline-safe.
  * HITL (destructive_fix)       -> propose_fix_task.human_input=True. Set per
    diagnosed failure via human_gated_fix= (only destructive failures pause).
  * Guardrails + output_pydantic (malformed_data) -> write_postmortem_task carries
    output_pydantic=Postmortem + guardrail=validate_postmortem + max_retries.
  * Tool reliability (slow_source) -> A2 max_retry_limit + per-call tool timeout.
  * Flows / conditional routing (multi_failure_cascade) -> sentinel.flow wraps
    this crew; the base path runs the plain hierarchical crew (no Flow overhead).

A STUB-LLM SEAM (no live API key required for wiring tests): pass an
``llm_override`` (a ``crewai.LLM`` subclass) and it is applied to the manager and
every specialist; ``create_llm`` returns an ``isinstance(x, LLM)`` object as-is,
so a scripted ``LLM`` subclass runs the real machinery without token calls.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task

from sentinel.guardrails import validate_postmortem
from sentinel.models import Diagnosis, Postmortem
from sentinel.tools import (
    ProfileRejects,
    ProposePatch,
    QueryDuckDB,
    QueryIncidentRAG,
    ReadDagsterLogs,
    ReadDbtRunResults,
    WritePostmortem,
)

# The disambiguation runbook backing the ambiguous_anomaly Knowledge unlock.
RUNBOOK_PATH = Path(__file__).resolve().parent / "knowledge" / "runbook.md"

# Failures whose remediation is destructive — the propose-fix task pauses for
# human approval (HITL) only for these (mirror of resolution.DESTRUCTIVE_*).
HUMAN_GATED_FAILURES: frozenset[str] = frozenset({"destructive_fix"})


def _load_knowledge_sources() -> list[Any]:
    """Build the runbook knowledge source. Imported lazily so the default crew
    build never touches the embedder (attaching knowledge embeds, needs a key).
    """
    from crewai.knowledge.source.string_knowledge_source import StringKnowledgeSource

    text = RUNBOOK_PATH.read_text(encoding="utf-8")
    return [StringKnowledgeSource(content=text)]


@CrewBase
class SentinelCrew:
    """Hierarchical Sentinel crew: A1 manager over the A2/A3 + A4/A5 squads."""

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"

    def __init__(
        self,
        llm_override: Any | None = None,
        *,
        memory: bool = False,
        with_knowledge: bool = False,
        human_gated_fix: bool = False,
    ) -> None:
        """Args:
        llm_override: An optional ``crewai.LLM`` (or scripted stub) applied to
            every agent. ``None`` keeps the tiered models from agents.yaml.
        memory: Enable crew memory (the recurring_incident unlock). Off by
            default — it needs an embedder/API key at runtime.
        with_knowledge: Attach the disambiguation runbook as a knowledge source
            (the ambiguous_anomaly unlock). Off by default — attaching it embeds
            the document, which needs a key.
        human_gated_fix: Mark the propose-fix task ``human_input=True`` (the
            destructive_fix HITL unlock). Set when the diagnosed failure is
            destructive; the trigger flips this per incident.
        """
        self._llm_override = llm_override
        self._memory = memory
        self._with_knowledge = with_knowledge
        self._human_gated_fix = human_gated_fix

    def _apply_llm(self, cfg: dict[str, Any]) -> dict[str, Any]:
        if self._llm_override is not None:
            cfg["llm"] = self._llm_override
        return cfg

    # --- A1 Manager (manager_agent — intentionally NOT an @agent method) ------
    def manager_agent_instance(self) -> Agent:
        """A1 Tech Lead: the hierarchical coordinator. Delegates, never detects.
        No tools (delegation is structural in the hierarchical process).
        """
        cfg = self._apply_llm(dict(self.agents_config["manager"]))
        return Agent(config=cfg, allow_delegation=True)

    # --- A2 Log Analyst (I1 Dagster logs, I2 dbt run results) -----------------
    @agent
    def log_analyst(self) -> Agent:
        cfg = self._apply_llm(dict(self.agents_config["log_analyst"]))
        return Agent(
            config=cfg,
            tools=[ReadDagsterLogs(), ReadDbtRunResults()],
            allow_delegation=False,
            # slow_source unlock: a stalled source makes log reads flaky, so the
            # log tool gets bounded retries at the agent level.
            max_retry_limit=2,
        )

    # --- A3 Data Profiler (I3 DuckDB silver/gold + rejects) -------------------
    @agent
    def data_profiler(self) -> Agent:
        cfg = self._apply_llm(dict(self.agents_config["data_profiler"]))
        return Agent(
            config=cfg,
            tools=[ProfileRejects(), QueryDuckDB()],
            allow_delegation=False,
        )

    # --- A4 Data Engineer (gated proposed patch, never applied to platform/) --
    @agent
    def data_engineer(self) -> Agent:
        cfg = self._apply_llm(dict(self.agents_config["data_engineer"]))
        return Agent(
            config=cfg,
            tools=[ProposePatch()],
            allow_delegation=False,
        )

    # --- A5 Incident Commander (I5 RAG recall + typed post-mortem) ------------
    @agent
    def incident_commander(self) -> Agent:
        cfg = self._apply_llm(dict(self.agents_config["incident_commander"]))
        return Agent(
            config=cfg,
            tools=[QueryIncidentRAG(), WritePostmortem()],
            allow_delegation=False,
        )

    # --- Investigation task chain --------------------------------------------
    @task
    def analyze_pipeline_logs_task(self) -> Task:
        return Task(config=self.tasks_config["analyze_pipeline_logs_task"])

    @task
    def profile_warehouse_task(self) -> Task:
        return Task(config=self.tasks_config["profile_warehouse_task"])

    @task
    def synthesize_diagnosis_task(self) -> Task:
        # The scoring hand-off: the typed Diagnosis.failure_key is what
        # sentinel.scoring consumes. output_pydantic takes the CLASS.
        return Task(
            config=self.tasks_config["synthesize_diagnosis_task"],
            output_pydantic=Diagnosis,
        )

    # --- Resolution task chain -----------------------------------------------
    @task
    def propose_fix_task(self) -> Task:
        # destructive_fix HITL unlock: human_input pauses for approval, but only
        # when the diagnosed failure is destructive (flipped per incident).
        return Task(
            config=self.tasks_config["propose_fix_task"],
            human_input=self._human_gated_fix,
        )

    @task
    def write_postmortem_task(self) -> Task:
        # malformed_data unlock: typed output + a validation guardrail force a
        # structured report even from garbage. 0.100.0 uses max_retries (NOT
        # guardrail_max_retries) and a singular guardrail= (verified).
        return Task(
            config=self.tasks_config["write_postmortem_task"],
            output_pydantic=Postmortem,
            guardrail=validate_postmortem,
            max_retries=2,
        )

    @crew
    def crew(self) -> Crew:
        """Build the hierarchical crew.

        ``self.agents`` / ``self.tasks`` are populated by ``@CrewBase`` from the
        ``@agent`` / ``@task`` methods (A2/A3/A4/A5 + the 5 tasks). The manager is
        added via ``manager_agent=`` and excluded from ``agents=`` per contract.
        Memory and knowledge are opt-in (they need an embedder/API key).
        """
        kwargs: dict[str, Any] = {
            "agents": self.agents,  # A2..A5; manager is injected by CrewAI
            "tasks": self.tasks,
            "process": Process.hierarchical,
            "manager_agent": self.manager_agent_instance(),
            "verbose": False,
            "memory": self._memory,  # recurring_incident unlock (opt-in)
        }
        if self._with_knowledge:
            # ambiguous_anomaly unlock (opt-in: attaching embeds the runbook).
            kwargs["knowledge_sources"] = _load_knowledge_sources()
        return Crew(**kwargs)
