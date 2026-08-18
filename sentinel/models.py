"""Typed contracts at the crew -> scoring seam.

The hierarchical crew's synthesize task emits a :class:`Diagnosis` via
``output_pydantic``; the scoring oracle (:mod:`sentinel.scoring`) consumes its
``failure_key`` / ``sub_failures`` / ``evidence_surface`` to grade against the
I4 ledger ground truth. Keeping this contract typed (rather than free text) is
what makes Component B *scorable* instead of merely asserted (R5).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class Diagnosis(BaseModel):
    """The crew's verdict for one incident, graded against I4 ground truth.

    Attributes:
        failure_key: The single ``failure_key`` the crew concluded. For a
            cascade this is ``"multi_failure_cascade"`` and the members go in
            :attr:`sub_failures`.
        sub_failures: For ``multi_failure_cascade`` only — the member failures
            the crew named. Empty for single-failure diagnoses.
        evidence_surface: The I-surface the diagnosis cites (e.g.
            ``"silver_orders_rejects"`` for ``negative_price``). The scoring
            evidence tier checks this against an expected-surface map; it is
            non-gating, so a lucky key with fabricated evidence is visibly
            flagged rather than rewarded.
        confidence: The crew's self-reported confidence (0.0-1.0). Reported,
            never used to gate the score.
        summary: A one-line human-readable summary of the diagnosis.
    """

    failure_key: str = Field(..., description="The diagnosed generator failure_key.")
    sub_failures: list[str] = Field(
        default_factory=list,
        description="Member failures for a multi_failure_cascade; empty otherwise.",
    )
    evidence_surface: str = Field(
        default="",
        description="The I-surface the diagnosis cites (e.g. silver_orders_rejects).",
    )
    confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Self-reported confidence; reported, never gates the score.",
    )
    summary: str = Field(default="", description="One-line human-readable summary.")


class Postmortem(BaseModel):
    """The crew's human-facing incident write-up (A5 output).

    Built here so the resolution-squad tasks (A4/A5, later phases) have a typed
    target; the base investigation path (A1/A2/A3) does not emit it yet.
    """

    incident_key: str = Field(..., description="The diagnosed failure_key.")
    root_cause: str = Field(..., description="The root cause in plain language.")
    evidence: str = Field(default="", description="The evidence that supports the root cause.")
    proposed_fix: str = Field(default="", description="The proposed (gated, never auto-applied) fix.")
    recurrence_note: str = Field(default="", description="Whether this is a repeat offender.")
