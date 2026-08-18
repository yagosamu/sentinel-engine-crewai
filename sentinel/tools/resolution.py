"""Resolution-squad tools — A4 Data Engineer and A5 Incident Commander.

These are the ONLY tools in the Sentinel that *write* anything, and the writes
are deliberately quarantined (R3, the one-way dependency):

  * :class:`ProposePatch` (A4) writes a *proposed* patch to ``sentinel/proposed/``
    and NOTHING else. It never edits ``platform/`` — the backbone it diagnoses is
    read-only to Component B. The patch is the weak B->A link: gated, advisory,
    never auto-applied (sketch line 66-67). A human reviews and applies it by
    hand, if at all.
  * :class:`WritePostmortem` (A5) writes the crew's human-facing post-mortem to
    ``sentinel/postmortems/``. The post-mortem is typed (``output_pydantic`` on
    the task) and guardrail-validated (the ``malformed_data`` unlock): even when
    the source data is garbage, the report that comes out is structured.

Both resolve their output roots from inside the ``sentinel/`` package tree, so a
patch can never escape into the backbone even if the caller passes a hostile
path (the filename is slugified; no directory traversal survives).
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

# Output roots, anchored inside the sentinel package (this file is
# sentinel/tools/resolution.py -> parents[1] == sentinel/). A4 may write only
# under PROPOSED_DIR; A5 only under POSTMORTEM_DIR. Neither is ever platform/.
_SENTINEL_ROOT = Path(__file__).resolve().parents[1]
PROPOSED_DIR = _SENTINEL_ROOT / "proposed"
POSTMORTEM_DIR = _SENTINEL_ROOT / "postmortems"

# Failures whose remediation is destructive (bulk overwrite) and therefore must
# pass through human approval before A4 even emits a proposal. The task that uses
# ProposePatch is marked human_input=True for these (config/tasks.yaml).
DESTRUCTIVE_FAILURE_KEYS: frozenset[str] = frozenset({"destructive_fix"})

_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def _slug(value: str) -> str:
    """Collapse arbitrary text to a safe filename fragment (no traversal)."""
    cleaned = _SLUG_RE.sub("-", value.strip().lower()).strip("-")
    return cleaned[:48] or "incident"


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


class ProposePatchArgs(BaseModel):
    failure_key: str = Field(..., description="The diagnosed generator failure_key the patch addresses.")
    title: str = Field(..., description="A short human title for the proposed fix.")
    patch_body: str = Field(..., description="The proposed dbt/Dagster/SQL change, as text. Advisory only.")
    rationale: str = Field(default="", description="Why this fix addresses the diagnosed root cause.")


class ProposePatch(BaseTool):
    """A4: write a *proposed* (gated, never auto-applied) patch to sentinel/proposed/.

    GUARANTEE (R3): this tool writes ONLY under ``sentinel/proposed/``. It does not
    and cannot edit the backbone (``platform/``). The output is a proposal a human
    must review and apply by hand. For destructive remediations the *task* gates
    on human approval (human_input=True) before this tool is ever called.
    """

    name: str = "propose_patch"
    description: str = (
        "Write a PROPOSED fix (a dbt/Dagster/SQL change as text) for a diagnosed "
        "failure to sentinel/proposed/. The patch is GATED and ADVISORY: it is "
        "never applied to the backbone (platform/) automatically. Returns the "
        "path of the written proposal. Use only after a root cause is confirmed."
    )
    args_schema: type[BaseModel] = ProposePatchArgs

    def _run(self, failure_key: str, title: str, patch_body: str, rationale: str = "") -> str:
        PROPOSED_DIR.mkdir(parents=True, exist_ok=True)
        is_destructive = failure_key in DESTRUCTIVE_FAILURE_KEYS
        name = f"{_timestamp()}-{_slug(failure_key)}.md"
        path = PROPOSED_DIR / name
        banner = (
            "> DESTRUCTIVE: this patch overwrites production data in bulk. It must "
            "NOT be applied without explicit human approval.\n\n"
            if is_destructive
            else "> Advisory proposal. Gated — review and apply by hand; never auto-applied.\n\n"
        )
        document = (
            f"# Proposed fix — `{failure_key}`\n\n"
            f"{banner}"
            f"**Title:** {title}\n\n"
            f"**Generated:** {_timestamp()} (Sentinel A4 Data Engineer)\n\n"
            f"## Rationale\n\n{rationale or '(none provided)'}\n\n"
            f"## Proposed change\n\n```\n{patch_body}\n```\n"
        )
        # Write under the sentinel tree only; resolve() + relative_to guards that
        # no crafted failure_key/title escaped the proposals dir.
        resolved = path.resolve()
        resolved.relative_to(PROPOSED_DIR.resolve())  # raises if traversal escaped
        resolved.write_text(document, encoding="utf-8")
        return json.dumps(
            {
                "ok": True,
                "proposed_path": str(resolved),
                "failure_key": failure_key,
                "destructive": is_destructive,
                "applied": False,  # ALWAYS false — proposals are never auto-applied (R3)
            }
        )


class WritePostmortemArgs(BaseModel):
    incident_key: str = Field(..., description="The diagnosed failure_key the post-mortem covers.")
    root_cause: str = Field(..., description="The root cause in plain language.")
    evidence: str = Field(default="", description="The evidence surface(s) that support the root cause.")
    proposed_fix: str = Field(default="", description="The gated, never-auto-applied proposed fix (summary).")
    recurrence_note: str = Field(default="", description="Whether this is a repeat offender (memory signal).")


class WritePostmortem(BaseTool):
    """A5: write the blameless post-mortem markdown to sentinel/postmortems/.

    The post-mortem is the crew's primary human-facing output. The task that
    drives it carries ``output_pydantic=Postmortem`` and a validation guardrail
    (the malformed_data unlock), so the report is typed and complete even when the
    underlying incident data is garbage.
    """

    name: str = "write_postmortem"
    description: str = (
        "Write a blameless incident post-mortem (markdown) to sentinel/postmortems/. "
        "Returns the written path. The report is typed and validated upstream; keep "
        "fields concise and never echo raw garbage data into structured fields."
    )
    args_schema: type[BaseModel] = WritePostmortemArgs

    def _run(
        self,
        incident_key: str,
        root_cause: str,
        evidence: str = "",
        proposed_fix: str = "",
        recurrence_note: str = "",
    ) -> str:
        POSTMORTEM_DIR.mkdir(parents=True, exist_ok=True)
        name = f"{_timestamp()}-{_slug(incident_key)}.md"
        path = POSTMORTEM_DIR / name
        document = (
            f"# Post-mortem — `{incident_key}`\n\n"
            f"**Generated:** {_timestamp()} (Sentinel A5 Incident Commander)\n\n"
            "_Blameless: this report addresses systems and signals, not people._\n\n"
            f"## Root cause\n\n{root_cause}\n\n"
            f"## Evidence\n\n{evidence or '(none cited)'}\n\n"
            f"## Proposed fix (gated — never auto-applied)\n\n{proposed_fix or '(none)'}\n\n"
            f"## Recurrence\n\n{recurrence_note or 'First observed occurrence.'}\n"
        )
        resolved = path.resolve()
        resolved.relative_to(POSTMORTEM_DIR.resolve())
        resolved.write_text(document, encoding="utf-8")
        return json.dumps({"ok": True, "postmortem_path": str(resolved), "incident_key": incident_key})
