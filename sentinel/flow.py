"""SentinelFlow — the multi_failure_cascade unlock (CrewAI Flows + routing).

This Flow exists for EXACTLY ONE failure: ``multi_failure_cascade``, where the
generator injects several independent failures at once (missing_customer +
volume_spike + schema_drift — src/gen/failures.py ``MultiFailureCascade``). A
single hierarchical crew answering with one ``failure_key`` cannot represent a
multi-dimensional incident, so the cascade gets a Flow whose ``@router`` fans the
investigation across squads and whose ``or_`` listener synthesises a Diagnosis
carrying BOTH ``failure_key="multi_failure_cascade"`` AND the member
``sub_failures``.

EVERYTHING ELSE stays on the plain hierarchical crew (:mod:`sentinel.crew`).
Flow-ifying the 13 single-failure paths would add state and latency for no gain
(anti-pattern: Flows where hierarchy suffices). The trigger only routes a cascade
incident here.

DETERMINISTIC TRIAGE (offline-safe). The triage step reads the REAL evidence
surfaces directly via the read-only tools (ProfileRejects on I3, ReadDbtRunResults
on I2) — not via an LLM. So the routing/fan-out wiring is verifiable WITHOUT an
API key (R5/HONESTY): the Flow proves it routes each detected dimension to the
right branch and assembles the correct sub_failures, and it never claims an LLM
diagnosed anything. A deeper LLM crew investigation can be layered on top when a
key is present, but the cascade diagnosis itself is grounded in observed evidence.

API verified against crewai 0.100.0 (Flow._execute_listeners): ``@router`` emits a
SINGLE route string — after a router runs, the executor sets the next trigger to
``self._method_outputs[-1]`` (one value), and ``_find_triggered_methods`` matches
it with ``trigger_method in methods``. A LIST return therefore does NOT fan out to
several ``@listen`` branches on this line (that is a newer-API behaviour; red flag
#1 — version skew — confirmed against the installed package). So the cascade is
built the 0.100.0 way: the router emits one PRIMARY route, the two squad branches
are CHAINED so both run, and ``synthesize`` (an ``or_`` listener) always reports
the full detected member set from ``self.state`` — the load-bearing output does
not depend on how many branches fired.

API verified present: ``Flow[StateModel]``, ``@start``, ``@router`` (single
string), ``@listen("route")``, ``@listen(method)``, ``@listen(or_(...))``,
``self.state``, ``kickoff()``.
"""

from __future__ import annotations

import json

from crewai.flow.flow import Flow, listen, or_, router, start
from pydantic import BaseModel, Field

from sentinel.models import Diagnosis
from sentinel.tools.logs import ReadDbtRunResults
from sentinel.tools.warehouse import ProfileRejects

# The members the cascade injector fires together (mirror of
# failures.MultiFailureCascade.inject). The triage probes for each.
CASCADE_MEMBERS = ("missing_customer", "volume_spike", "schema_drift")

# Route names the @router emits; each maps to a squad branch.
ROUTE_PIPELINE = "pipeline_error"  # A2 territory: schema_drift / slow_source
ROUTE_DATA = "data_defect"  # A3 territory: quarantined / statistical defects

# Which detected members imply which route.
_PIPELINE_MEMBERS = frozenset({"schema_drift", "slow_source"})


class CascadeState(BaseModel):
    """Flow state for one cascade investigation (Pydantic-typed per the Flow API)."""

    detected: list[str] = Field(default_factory=list)  # members found in evidence
    routes: list[str] = Field(default_factory=list)  # route strings the router emitted
    branches_ran: list[str] = Field(default_factory=list)  # which @listen branches fired
    diagnosis: Diagnosis | None = None


class SentinelFlow(Flow[CascadeState]):
    """Conditional-routing Flow for multi_failure_cascade.

    ``triage`` reads evidence -> ``route`` fans to pipeline/data branches ->
    ``synthesize`` (an ``or_`` listener) emits the multi-failure Diagnosis.
    """

    def __init__(self, profile_tool: ProfileRejects | None = None, dbt_tool: ReadDbtRunResults | None = None) -> None:
        # Tools are injectable so tests can supply fixtures (no live warehouse).
        super().__init__()
        self._profile = profile_tool or ProfileRejects()
        self._dbt = dbt_tool or ReadDbtRunResults()

    # --- @start: deterministic, evidence-grounded triage ---------------------
    @start()
    def triage(self) -> str:
        """Probe each cascade member against its real evidence surface (I2/I3)."""
        detected: list[str] = []
        for member in CASCADE_MEMBERS:
            if member == "schema_drift":
                # schema_drift is a pipeline error (I2) AND an accepted flag (I3).
                if self._member_found_in_dbt() or self._member_found_in_warehouse(member):
                    detected.append(member)
            elif self._member_found_in_warehouse(member):
                detected.append(member)
        self.state.detected = detected
        return "triaged"

    def _member_found_in_warehouse(self, member: str) -> bool:
        try:
            payload = json.loads(self._profile._run(failure_key=member))
        except Exception:  # noqa: BLE001 - a tool error is "not found" for routing
            return False
        return bool(payload.get("found"))

    def _member_found_in_dbt(self) -> bool:
        try:
            payload = json.loads(self._dbt._run())
        except Exception:  # noqa: BLE001
            return False
        return bool(payload.get("found"))

    # --- @router: emit ONE primary route (0.100.0 single-output semantics) ---
    @router(triage)
    def route(self) -> str:
        """Return the primary route. Pipeline errors take precedence (they crash
        the run); the data branch is chained off the pipeline branch so BOTH run
        regardless of which is primary. The full routing intent is recorded in
        ``state.routes`` for observability.
        """
        routes: list[str] = []
        if any(m in _PIPELINE_MEMBERS for m in self.state.detected):
            routes.append(ROUTE_PIPELINE)
        if any(m not in _PIPELINE_MEMBERS for m in self.state.detected):
            routes.append(ROUTE_DATA)
        self.state.routes = routes
        # One route only on 0.100.0. Pipeline first if present, else data; if
        # neither member was detected, still drive the data branch so synthesize
        # runs and emits an (empty-member) cascade verdict rather than stalling.
        if ROUTE_PIPELINE in routes:
            return ROUTE_PIPELINE
        return ROUTE_DATA

    # --- Squad branches (CHAINED so both run on a multi-dimensional cascade) --
    @listen(ROUTE_PIPELINE)
    def investigate_pipeline(self) -> str:
        # A2 territory. Deterministic record of the branch firing; a deeper LLM
        # crew investigation can attach here when a key is present.
        self.state.branches_ran.append(ROUTE_PIPELINE)
        # Chain into the data branch when the cascade also carries a data defect,
        # so a pipeline-primary cascade still runs A3's investigation.
        if ROUTE_DATA in self.state.routes:
            return ROUTE_DATA
        return ROUTE_PIPELINE

    @listen(or_(ROUTE_DATA, investigate_pipeline))
    def investigate_data(self) -> str:
        # A3 territory. Fires when the router chose the data route directly OR the
        # pipeline branch chained into it. Idempotent — record-once.
        if ROUTE_DATA not in self.state.branches_ran:
            self.state.branches_ran.append(ROUTE_DATA)
        return ROUTE_DATA

    # --- Synthesis: fires after EITHER squad branch (or_) --------------------
    @listen(or_(investigate_pipeline, investigate_data))
    def synthesize(self) -> Diagnosis:
        """Emit the cascade Diagnosis: top-level cascade + named sub_failures.

        Idempotent across the two branch firings — it always reports the full
        detected member set, so the scorer's cascade-overlap tier can grade it.
        """
        members = list(self.state.detected)
        surfaces = "silver_orders_rejects + silver_orders.is_late/_schema_drift + dbt run_results"
        diagnosis = Diagnosis(
            failure_key="multi_failure_cascade",
            sub_failures=members,
            evidence_surface=surfaces,
            confidence=0.6 if members else 0.0,
            summary=(
                f"multi_failure_cascade: detected members {sorted(members)} across "
                f"routes {self.state.routes}"
            ),
        )
        self.state.diagnosis = diagnosis
        return diagnosis


def diagnose_cascade(
    profile_tool: ProfileRejects | None = None,
    dbt_tool: ReadDbtRunResults | None = None,
) -> Diagnosis:
    """Run the cascade Flow and return its typed Diagnosis.

    Entry point the trigger calls when the incident is a multi_failure_cascade.
    Deterministic and offline-safe (reads real evidence surfaces, no LLM).
    """
    flow = SentinelFlow(profile_tool=profile_tool, dbt_tool=dbt_tool)
    flow.kickoff()
    return flow.state.diagnosis or Diagnosis(
        failure_key="multi_failure_cascade",
        sub_failures=[],
        summary="cascade flow produced no diagnosis",
    )
