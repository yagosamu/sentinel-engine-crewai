"""Task guardrails — the malformed_data unlock.

A CrewAI task ``guardrail`` is a callable ``(TaskOutput) -> tuple[bool, Any]``
(verified against crewai 0.100.0: ``Task`` has a singular ``guardrail`` field and
``max_retries``; the newer ``guardrails`` list / ``guardrail_max_retries`` are NOT
present in this line). On ``(False, msg)`` CrewAI re-runs the task up to
``max_retries`` with the message as feedback; on ``(True, value)`` it accepts.

:func:`validate_postmortem` is the gate that makes the ``malformed_data`` failure
tractable: even when the source incident data is garbage, the post-mortem that
leaves the crew must be a *typed, populated* :class:`~sentinel.models.Postmortem`
— not an empty shell and not a wall of raw noise. The guardrail rejects a
post-mortem missing a root cause, or one that smuggled non-printable garbage into
a structured field, forcing the agent to summarise instead of echo.
"""

from typing import Any, Tuple  # noqa: UP035 - crewai validates the literal annotation Tuple[bool, Any]

from sentinel.models import Postmortem

# Heuristic markers of un-summarised garbage leaking into a structured field
# (mirrors the noise the malformed_data injector writes: src/gen/failures.py).
_GARBAGE_MARKERS = ("<script>", "0x00", "\x00", "�")
_MIN_ROOT_CAUSE_CHARS = 12


def _looks_like_raw_garbage(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in _GARBAGE_MARKERS)


def validate_postmortem(result: Any) -> Tuple[bool, Any]:  # noqa: UP006 - crewai matches the literal Tuple annotation
    """Guardrail: accept only a typed, populated, non-garbage Postmortem.

    Args:
        result: the task's ``TaskOutput``. With ``output_pydantic=Postmortem`` its
            ``.pydantic`` is a :class:`Postmortem`; we validate that object (and
            fall back to parsing ``.raw`` if the typed slot is absent).

    Returns:
        ``(True, Postmortem)`` when the report is well-formed, else
        ``(False, message)`` so CrewAI retries with the message as feedback.
    """
    pm = getattr(result, "pydantic", None)
    if pm is None:
        # No typed output produced — try to coerce the raw text, else reject.
        raw = getattr(result, "raw", None) or str(result)
        try:
            pm = Postmortem.model_validate_json(raw)
        except Exception:  # noqa: BLE001 - any parse failure is a guardrail reject
            return (False, "Post-mortem must be a typed Postmortem object with all fields populated.")

    if not isinstance(pm, Postmortem):
        return (False, "Post-mortem output is not a Postmortem; emit the typed structure.")

    if not pm.incident_key.strip():
        return (False, "Post-mortem is missing incident_key (the diagnosed failure_key).")

    if len(pm.root_cause.strip()) < _MIN_ROOT_CAUSE_CHARS:
        return (False, "root_cause is too short — explain the cause in plain language, do not leave it blank.")

    for field_name in ("root_cause", "evidence", "proposed_fix", "recurrence_note"):
        value = getattr(pm, field_name, "") or ""
        if _looks_like_raw_garbage(value):
            return (
                False,
                f"{field_name} contains raw garbage data — SUMMARISE the malformed input, do not echo it verbatim.",
            )

    return (True, pm)
