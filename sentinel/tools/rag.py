"""A5 Incident Commander tool — the I5 incident RAG store (B2, self-built).

The historical-incident store is Component B's OWN artefact (I5 in the interface
table): it cold-starts EMPTY and accumulates as the crew writes post-mortems
(sketch line 121-127). Until it has its own history it can be BOOTSTRAPPED from
the ``injected_incidents`` ledger (I4) — every past incident is a prior the
commander can recall ("we have seen negative_price 3 times before").

This is a deliberately LIGHTWEIGHT, dependency-free retriever, not a vector DB:
it reads the I4 ledger read-only and ranks prior incidents for a query by token
overlap on ``failure_key`` + ``detail``. That is enough to power the two things
the design actually needs from I5:

  * recurrence recognition (the ``recurring_incident`` Memory story's data side),
  * "similar past incident" recall for the post-mortem.

It is READ-ONLY against Postgres (R3) — it queries the ledger, never writes it.
A real vector store can replace the ranking later without changing the tool's
contract (args in, ranked incidents out).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

from src.gen.repository import session

_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def _tokens(text: str) -> set[str]:
    return set(_TOKEN_RE.findall(text.lower()))


@dataclass(frozen=True, slots=True)
class _Incident:
    failure_key: str
    detail: str
    detected_by: str
    injected_at: str

    def as_dict(self) -> dict[str, str]:
        return {
            "failure_key": self.failure_key,
            "detail": self.detail,
            "detected_by": self.detected_by,
            "injected_at": self.injected_at,
        }


def _load_ledger(limit: int) -> list[_Incident]:
    """Read prior incidents from the I4 ledger (read-only) to bootstrap the RAG."""
    conn = session()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT failure_key, detail, detected_by, injected_at "
                "FROM injected_incidents ORDER BY injected_at DESC, incident_id DESC LIMIT %s",
                (limit,),
            )
            return [_Incident(r[0], r[1], r[2], str(r[3])) for r in cur.fetchall()]
    finally:
        conn.close()


def rank_incidents(query: str, incidents: list[_Incident], top_k: int) -> list[tuple[float, _Incident]]:
    """Pure ranking helper (token-overlap similarity). Exposed for unit testing."""
    q = _tokens(query)
    scored: list[tuple[float, _Incident]] = []
    for inc in incidents:
        corpus = _tokens(f"{inc.failure_key} {inc.detail}")
        if not corpus:
            continue
        overlap = len(q & corpus)
        if overlap == 0:
            continue
        score = overlap / (len(q | corpus) or 1)  # Jaccard
        scored.append((score, inc))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return scored[:top_k]


class QueryIncidentRAGArgs(BaseModel):
    description: str = Field(
        ...,
        description="A free-text description of the current incident to find similar past incidents for.",
    )
    top_k: int = Field(default=3, ge=1, le=20, description="How many similar past incidents to return.")
    bootstrap_limit: int = Field(
        default=200, ge=1, le=2000, description="How many ledger rows to consider as the prior history."
    )


class QueryIncidentRAG(BaseTool):
    """I5: recall similar past incidents from the historical store (read-only).

    Cold-starts empty; bootstraps from the I4 ledger. Returns the ranked similar
    incidents plus a recurrence count for the closest failure_key, so the
    commander can write "seen N times before" instead of cold-starting.
    """

    name: str = "query_incident_rag"
    description: str = (
        "Search the historical incident store (I5) for incidents similar to a "
        "description. Returns ranked past incidents and a recurrence count for the "
        "most similar failure_key. Cold-starts empty; bootstraps from the "
        "injected_incidents ledger. Read-only."
    )
    args_schema: type[BaseModel] = QueryIncidentRAGArgs

    def _run(self, description: str, top_k: int = 3, bootstrap_limit: int = 200) -> str:
        incidents = _load_ledger(bootstrap_limit)
        if not incidents:
            return json.dumps(
                {
                    "found": False,
                    "detail": "incident store is empty (cold start; no prior history to recall)",
                    "matches": [],
                    "recurrence": {},
                }
            )
        ranked = rank_incidents(description, incidents, top_k)
        matches = [{"score": round(score, 4), **inc.as_dict()} for score, inc in ranked]
        recurrence: dict[str, int] = {}
        if ranked:
            top_key = ranked[0][1].failure_key
            recurrence = {
                "failure_key": top_key,
                "times_seen": sum(1 for inc in incidents if inc.failure_key == top_key),
            }
        return json.dumps(
            {
                "found": bool(matches),
                "matches": matches,
                "recurrence": recurrence,
                "history_size": len(incidents),
            },
            default=str,
        )
