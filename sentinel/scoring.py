"""I4 ground-truth scoring oracle for Sentinel diagnoses.

The oracle reads the ``injected_incidents`` Postgres ledger (written by the chaos
generator, I4) and grades a crew :class:`~sentinel.models.Diagnosis` against it.
It SCORES against ground truth; it never asserts the crew is "correct" (R5). A run
the crew could not complete returns a NO-RUN result, never a fabricated score
(HONESTY RULE).

THE GRADED RUBRIC (deterministic, auditable — every threshold is a module
constant):

  Tier 1 — DIAGNOSIS MATCH (gating, 0.0-1.0):
    1.0 EXACT   diagnosis.failure_key == oracle.failure_key
    0.7 ALIASED a registry-justified equivalence (recurring_incident <->
                negative_price, because the recurring injector writes negative
                prices). The alias map is a TINY explicit dict, never fuzzy text.
    0.5 .. 1.0  CASCADE-PARTIAL for multi_failure_cascade: base 0.5 plus
                0.5 * (named members ∩ actual members) / actual members.
    0.0 MISS    none of the above.

  Tier 2 — EVIDENCE QUALITY (NON-gating, reported separately): did the diagnosis
    cite the correct I-surface? Catches "right key, fabricated reasoning" — a
    lucky guess scores match=1.0 evidence=0.0, which is visibly flagged, not
    rewarded.

The legacy ``score_diagnosis(key) -> str`` API is kept as a thin shim over the
new path so existing tests stay valid.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sentinel.models import Diagnosis
from src.gen.repository import session

# --- rubric constants (auditable; tune here, nowhere else) -------------------
EXACT_SCORE = 1.0
ALIAS_SCORE = 0.7
CASCADE_BASE = 0.5
CASCADE_MEMBER_WEIGHT = 0.5
MISS_SCORE = 0.0

CASCADE_KEY = "multi_failure_cascade"

# Registry-justified equivalences ONLY (keep this tiny — see red flag #5). The
# recurring_incident injector inserts negative_price rows, so a crew that says
# "negative_price" for a recurring incident is substantively correct.
ALIAS_MAP: dict[str, set[str]] = {
    "recurring_incident": {"negative_price"},
    "negative_price": {"recurring_incident"},
}

# Expected I-surface per failure_key (Tier 2 evidence check). Mirrors
# sentinel.tools.warehouse.DETECTION_MAP surfaces + the A2 log surfaces.
#
# Two deliberate asymmetries vs DETECTION_MAP (warehouse.py), so the maps do not
# line up key-for-key:
#   * slow_source is present HERE ('dagster_logs') but absent from DETECTION_MAP —
#     it is an A2/I1 LOG-surface failure, not an A3/I3 warehouse probe, so its
#     evidence surface lives in scoring even though it has no ProfileRejects probe.
#     (schema_drift is in BOTH: A2-log primary + A3 _schema_drift corroboration.)
#   * ambiguous_anomaly and multi_failure_cascade are absent from BOTH maps by
#     design — ambiguous_anomaly is Knowledge/RAG-only (so a correct diagnosis
#     scores evidence=0.0, a known limitation), and cascade is graded by the
#     dedicated cascade tier (_score_cascade), not a single expected surface.
EXPECTED_SURFACE: dict[str, str] = {
    "negative_price": "silver_orders_rejects",
    "missing_customer": "silver_orders_rejects",
    "invalid_quantity": "silver_orders_rejects",
    "duplicate_order": "silver_orders_rejects",
    "malformed_data": "silver_orders_rejects",
    "destructive_fix": "silver_orders_rejects",
    "recurring_incident": "silver_orders_rejects",
    "orphan_payment": "silver_payments_rejects",
    "late_arrival": "silver_orders.is_late",
    "schema_drift": "silver_orders._schema_drift",
    "volume_spike": "silver_orders.count",
    "slow_source": "dagster_logs",
}


@dataclass(frozen=True, slots=True)
class ScoreResult:
    """A graded score for one diagnosis against the I4 oracle.

    ``tier`` is the qualitative bucket ('exact' / 'alias' / 'cascade' / 'miss' /
    'no-run'). ``diagnosis_score`` is the gating Tier-1 value; ``evidence_score``
    is the non-gating Tier-2 value; ``matched`` is the oracle key(s) compared
    against; ``detail`` is a human-readable explanation.
    """

    diagnosis_score: float
    evidence_score: float
    tier: str
    matched: str
    detail: str
    oracle_keys: list[str] = field(default_factory=list)

    @property
    def is_correct(self) -> bool:
        """True only on an exact Tier-1 match (back-compat for the string API)."""
        return self.diagnosis_score >= EXACT_SCORE


def _no_run(detail: str) -> ScoreResult:
    return ScoreResult(
        diagnosis_score=0.0,
        evidence_score=0.0,
        tier="no-run",
        matched="",
        detail=detail,
        oracle_keys=[],
    )


def _fetch_oracle_keys(since: datetime | None) -> list[str]:
    """Read the I4 ledger. With ``since`` -> all rows in the run window (cascade
    needs every member); without -> the single latest failure_key.
    """
    conn = session()
    try:
        with conn.cursor() as cur:
            if since is None:
                cur.execute(
                    "SELECT failure_key FROM injected_incidents "
                    "ORDER BY injected_at DESC, incident_id DESC LIMIT 1"
                )
                row = cur.fetchone()
                return [row[0]] if row else []
            cur.execute(
                "SELECT failure_key FROM injected_incidents "
                "WHERE injected_at >= %s ORDER BY injected_at ASC, incident_id ASC",
                (since,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _evidence_score(diagnosis: Diagnosis, oracle_key: str) -> float:
    """Non-gating: 1.0 if the cited surface matches the expected surface for the
    oracle key, else 0.0. An empty/absent surface scores 0.0 (flagged honestly).
    """
    expected = EXPECTED_SURFACE.get(oracle_key, "")
    if not expected or not diagnosis.evidence_surface:
        return 0.0
    return 1.0 if diagnosis.evidence_surface.strip() == expected else 0.0


def _score_cascade(diagnosis: Diagnosis, oracle_keys: list[str]) -> ScoreResult:
    members = [k for k in oracle_keys if k != CASCADE_KEY]
    actual = set(members)
    named = set(diagnosis.sub_failures)
    overlap = len(named & actual)
    fraction = (overlap / len(actual)) if actual else 0.0
    score = CASCADE_BASE + CASCADE_MEMBER_WEIGHT * fraction
    return ScoreResult(
        diagnosis_score=round(score, 4),
        evidence_score=0.0,  # cascade evidence is judged per-member elsewhere
        tier="cascade",
        matched=CASCADE_KEY,
        detail=(
            f"cascade: named {sorted(named)} vs actual members {sorted(actual)} "
            f"({overlap}/{len(actual) or 0} overlap)"
        ),
        oracle_keys=oracle_keys,
    )


def score_run(diagnosis: Diagnosis | None, since: datetime | None = None) -> ScoreResult:
    """Grade a :class:`Diagnosis` against the I4 ledger over the run window.

    Args:
        diagnosis: The crew's typed diagnosis, or ``None`` if the crew could not
            run (no API key / crash) — yields a NO-RUN result, never a fake score.
        since: The run-window start captured at trigger time. ``None`` falls back
            to "the single latest incident" (back-compat path). For cascade and
            reproducibility (R7), always pass the captured cursor.

    Returns:
        A :class:`ScoreResult`. The score is REPORTED, not asserted correct.
    """
    if diagnosis is None:
        return _no_run("crew produced no diagnosis (no run / no API key)")

    oracle_keys = _fetch_oracle_keys(since)
    if not oracle_keys:
        return _no_run("I4 ledger has no incident in the scored window")

    # Cascade: the oracle window holds one row per member.
    if CASCADE_KEY in oracle_keys or diagnosis.failure_key == CASCADE_KEY:
        return _score_cascade(diagnosis, oracle_keys)

    # Single failure: compare against the most recent (last) key in the window.
    oracle_key = oracle_keys[-1]
    evidence = _evidence_score(diagnosis, oracle_key)

    if diagnosis.failure_key == oracle_key:
        return ScoreResult(
            diagnosis_score=EXACT_SCORE,
            evidence_score=evidence,
            tier="exact",
            matched=oracle_key,
            detail=f"exact match on '{oracle_key}'",
            oracle_keys=oracle_keys,
        )

    if oracle_key in ALIAS_MAP.get(diagnosis.failure_key, set()):
        return ScoreResult(
            diagnosis_score=ALIAS_SCORE,
            evidence_score=evidence,
            tier="alias",
            matched=oracle_key,
            detail=f"alias match: '{diagnosis.failure_key}' ~ '{oracle_key}'",
            oracle_keys=oracle_keys,
        )

    return ScoreResult(
        diagnosis_score=MISS_SCORE,
        evidence_score=evidence,
        tier="miss",
        matched=oracle_key,
        detail=f"miss: diagnosed '{diagnosis.failure_key}', oracle '{oracle_key}'",
        oracle_keys=oracle_keys,
    )


def score_diagnosis(diagnosis_key: str) -> str:
    """Score a diagnosis key against the latest I4 incident (legacy string API).

    Thin shim over :func:`score_run`: builds a minimal :class:`Diagnosis` from the
    bare key and returns ``'correct'`` iff Tier-1 is an EXACT match, else
    ``'incorrect'`` (including an empty ledger). Preserved so existing callers and
    tests keep working.
    """
    result = score_run(Diagnosis(failure_key=diagnosis_key), since=None)
    return "correct" if result.is_correct else "incorrect"
