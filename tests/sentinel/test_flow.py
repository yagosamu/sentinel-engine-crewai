"""L1 tests for the cascade Flow — the multi_failure_cascade unlock.

The Flow is DETERMINISTIC and offline-safe: ``triage`` reads the real evidence
surfaces via the read-only tools (here, fixture stand-ins), ``route`` picks the
0.100.0-compatible single primary route, the squad branches chain so both run, and
``synthesize`` emits a typed Diagnosis carrying the full detected member set. No
LLM is involved, so the routing/fan-out is fully verifiable without a key (R5).

REGRESSION GUARD: on crewai 0.100.0 a ``@router`` that returns a LIST does NOT fan
out to several ``@listen`` branches (the executor uses a single next-trigger). These
tests pin that the Flow still captures ALL detected members regardless of which
branch is primary — the behaviour the list-return design silently lost.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("sentinel.flow", reason="Sentinel flow not importable")

from sentinel.flow import CASCADE_MEMBERS, diagnose_cascade  # noqa: E402


class _FakeProfile:
    """ProfileRejects stand-in: 'found' iff the probed key is in the seeded set."""

    def __init__(self, found: set[str]) -> None:
        self._found = found

    def _run(self, failure_key: str, **_: object) -> str:
        return json.dumps({"found": failure_key in self._found})


class _FakeDbt:
    """ReadDbtRunResults stand-in: a single boolean 'found' (schema_drift in dbt)."""

    def __init__(self, found: bool) -> None:
        self._found = found

    def _run(self, **_: object) -> str:
        return json.dumps({"found": self._found})


def test_cascade_members_constant_matches_injector() -> None:
    # The Flow must probe exactly the members the cascade injector fires
    # (src/gen/failures.py MultiFailureCascade).
    assert set(CASCADE_MEMBERS) == {"missing_customer", "volume_spike", "schema_drift"}


def test_cascade_captures_all_three_members() -> None:
    dx = diagnose_cascade(
        profile_tool=_FakeProfile({"missing_customer", "volume_spike", "schema_drift"}),
        dbt_tool=_FakeDbt(True),
    )
    assert dx.failure_key == "multi_failure_cascade"
    assert set(dx.sub_failures) == {"missing_customer", "volume_spike", "schema_drift"}
    assert dx.confidence > 0.0


def test_cascade_data_only_when_no_pipeline_member() -> None:
    # missing_customer + volume_spike, no schema_drift -> data route only, but both
    # data members still captured.
    dx = diagnose_cascade(
        profile_tool=_FakeProfile({"missing_customer", "volume_spike"}),
        dbt_tool=_FakeDbt(False),
    )
    assert dx.failure_key == "multi_failure_cascade"
    assert set(dx.sub_failures) == {"missing_customer", "volume_spike"}


def test_cascade_pipeline_only_via_dbt_signal() -> None:
    # schema_drift visible only in dbt run_results (pipeline route), nothing in the
    # warehouse -> the pipeline member is still captured.
    dx = diagnose_cascade(
        profile_tool=_FakeProfile(set()),
        dbt_tool=_FakeDbt(True),
    )
    assert dx.failure_key == "multi_failure_cascade"
    assert dx.sub_failures == ["schema_drift"]


def test_cascade_pipeline_primary_still_runs_data_branch() -> None:
    # schema_drift (pipeline, primary route) + missing_customer (data) -> the
    # pipeline branch chains into the data branch so BOTH members survive. This is
    # the exact case the list-return router used to drop.
    dx = diagnose_cascade(
        profile_tool=_FakeProfile({"missing_customer", "schema_drift"}),
        dbt_tool=_FakeDbt(True),
    )
    assert set(dx.sub_failures) == {"missing_customer", "schema_drift"}


def test_cascade_no_members_is_empty_but_typed() -> None:
    # Nothing detected -> a typed cascade verdict with empty members (not a crash).
    dx = diagnose_cascade(profile_tool=_FakeProfile(set()), dbt_tool=_FakeDbt(False))
    assert dx.failure_key == "multi_failure_cascade"
    assert dx.sub_failures == []
    assert dx.confidence == 0.0
