# ADR-0002: Sentinel engine — locked decisions for Component B

**Status:** Accepted
**Date:** 2026-06-30

> Scope: this ADR records the decisions that gate Component B (the probabilistic
> Sentinel engine, `sentinel/`). The two-component split, the one-way A→B
> dependency (R3), the verify-by-scoring rule (R5), the all-14-failures rule (R6),
> and the reversibility rule (R7) are defined in
> [`../../CLAUDE.md`](../../CLAUDE.md) and
> [`../../.claude/rules/agent-operating-rules.md`](../../.claude/rules/agent-operating-rules.md)
> and are referenced, not restated, below. The companion record for the backbone
> is [`0001-analytical-backbone.md`](0001-analytical-backbone.md).

## Context

Component B watches the analytical backbone (Component A), diagnoses the 14
failures the chaos generator injects into the source, and proposes fixes. Unlike
A — which is verified by assertion against real numbers — B is **probabilistic**:
its output is an LLM crew's diagnosis, which can only be *scored against ground
truth*, never asserted correct (R5). Several topology and verification choices
proved load-bearing enough to record because anyone extending the crew will
re-litigate them. This ADR freezes the five decisions the now-built crew (79
passed / 6 skipped offline) depends on. All five uphold the one-way A→B seam (R3):
B reads A's exhaust read-only via the I1–I5 interface and writes nothing back into
the backbone.

---

## Decision 1 — hierarchical Process with a custom `manager_agent`

The crew runs `Process.hierarchical` with a **custom manager agent (A1)** that is
*not* a member of the specialist roster. A1 is built by
`SentinelCrew.manager_agent_instance()` and passed via `manager_agent=`; CrewAI
injects it into the run, so it never appears in `agents=[...]`.

- A1 has `allow_delegation=True` and **no tools** — delegation is structural in
  the hierarchical process; the manager coordinates, it does not detect.
- The specialists (A2 Log Analyst, A3 Data Profiler, A4 Data Engineer, A5
  Incident Commander) go in `agents=[]` with `allow_delegation=False` — only the
  manager delegates.
- The CrewAI 0.100.0 contract (verified against the installed package) is precise:
  with a custom `manager_agent` you pass `manager_agent=` and do **not** also pass
  `manager_llm`; the manager must not be listed in `agents=`. Violating either
  breaks the hierarchical loop.

**Why:** the failures split cleanly into pipeline errors (A2 territory: I1/I2) and
data defects (A3 territory: I3), with a multi-dimensional cascade that needs a
coordinator to assign each member to the right squad. A manager that delegates —
rather than a flat sequential pipeline — is the topology that matches the problem
and unlocks `multi_failure_cascade` routing (Decision 3).

---

## Decision 2 — the scoring rubric: evidence + cascade, not exact-match

The I4 oracle grades a typed `Diagnosis` against the `injected_incidents` ledger
with a **two-tier, deterministic** rubric (every threshold is a module constant in
`sentinel/scoring.py`), not a single exact-match boolean:

- **Tier 1 — diagnosis match (gating, 0.0–1.0):** `1.0` exact; `0.7` for the one
  registry-justified alias (`recurring_incident ↔ negative_price`, because the
  recurring injector writes negative-price rows); `0.5 + 0.5 × overlap` for a
  cascade; `0.0` miss. The alias map is a tiny explicit dict, never fuzzy text.
- **Tier 2 — evidence quality (non-gating, reported separately):** did the
  diagnosis cite the correct I-surface? A lucky guess with fabricated reasoning
  scores `match=1.0 evidence=0.0` — visibly flagged, not rewarded.
- A diagnosis the crew could not produce returns a NO-RUN result, never a
  fabricated score (the honesty rule).

The legacy `score_diagnosis(key) -> "correct"|"incorrect"` API is preserved as a
thin shim over `score_run` (exact Tier-1 only) so existing callers and tests keep
working.

**Why:** exact-match alone is both too harsh and too lenient. Too harsh, because
"negative_price" for a recurring incident is substantively right; too lenient,
because a key that happens to match while citing the wrong surface is a lucky
guess that exact-match would reward. Partial-credit Tiers make a probabilistic
crew's quality *measurable* (R5) — a 0.667 cascade or a 0.7 alias is honest
signal, not a pass/fail that hides what actually happened.

---

## Decision 3 — `multi_failure_cascade` gets a Flow; everything else does not

The 14th failure — where the generator fires `missing_customer` +
`volume_spike` + `schema_drift` at once — is handled by a dedicated CrewAI Flow
(`sentinel/flow.py`), while the other 13 single-failure paths stay on the plain
hierarchical crew.

- A single hierarchical crew answering with one `failure_key` cannot represent a
  multi-dimensional incident. The Flow's `@router` fans the investigation across a
  pipeline branch (A2) and a data branch (A3) and its `or_` listener synthesizes a
  `Diagnosis` carrying `failure_key="multi_failure_cascade"` **and** the member
  `sub_failures`.
- The Flow triage reads the **real** evidence surfaces directly via the read-only
  tools (`ProfileRejects` on I3, `ReadDbtRunResults` on I2) — **not** via an LLM —
  so the routing/fan-out wiring is verifiable offline and the cascade is always
  scorable without an API key. It never claims an LLM diagnosed anything.
- The 0.100.0 Flow API was verified against the installed package: `@router` emits
  a *single* route string (a list does not fan out on this line — that is newer-API
  behaviour). So the router emits one primary route (pipeline first, as those crash
  the run) and the two squad branches are *chained* so both run on a
  multi-dimensional cascade; `synthesize` always reports the full detected member
  set from `self.state`, so the load-bearing output is independent of how many
  branches fired.

**Why:** Flows where hierarchy suffices is an anti-pattern — it adds state and
latency for no gain. Only the cascade genuinely needs conditional routing, so only
the cascade pays for a Flow.

---

## Decision 4 — the B1 trigger polls I4; it does not hook a webhook

The trigger (`sentinel/trigger.py`) **polls** the `injected_incidents` ledger (I4)
for new rows rather than hooking a Dagster failure webhook.

- Only ~2 of the 14 failures actually crash a Dagster run; the other ~12 are
  *quarantined* rows in `silver_*_rejects` — the backbone run **succeeds** (that is
  the whole point of the quarantine-not-drop decision in ADR-0001). A failure
  webhook would never fire for those 12.
- Polling the **same table the scorer reads** keeps the trigger and the oracle
  consistent: every injected incident is both a trigger *and* a scorable event, and
  the loop is a pure function of ledger state — trivially reproducible in tests with
  a fixed `since` cursor (R7).
- The trigger reads I4 read-only via `src.gen.repository.session()` and writes
  nothing to Postgres or `platform/` (R3). It dispatches a single distinct
  `failure_key` to the base crew and several distinct keys to the cascade Flow (the
  cascade injector writes one ledger row per member and no top-level row, so the
  trigger recognises a cascade by simultaneous arrival of distinct members).

**Why:** a webhook keyed on run failure would be blind to most of the failure
registry. The ledger is the one surface that sees every injected incident, and
sharing it between trigger and oracle makes the inject→detect→score loop a
reproducible pure function instead of an event-timing race.

---

## Decision 5 — the B→A seam is a gated proposal; A4 never auto-applies

The only B→A link is an **advisory, gated** proposed patch. A4's `ProposePatch`
writes to `sentinel/proposed/` and nothing else; A5's `WritePostmortem` writes to
`sentinel/postmortems/`. Neither ever edits `platform/`.

- Both output roots are anchored inside the `sentinel/` package tree; the filename
  is slugified and re-checked with `relative_to`, so a crafted `failure_key` or
  title cannot traverse out of the sentinel directory (R8-style path discipline).
- The proposal carries `applied: false` always — it is reviewed and applied by a
  human, by hand, if at all.
- A **destructive** remediation (`destructive_fix`) does not even emit a proposal
  unattended: the propose-fix task is marked `human_input=True` (the HITL unlock),
  flipped per-incident by the trigger, so it pauses for explicit human approval
  first. The proposal banner states the patch is destructive.

**Why:** Component B is read-only over Component A (R3). An autonomous crew that
could mutate the production backbone would violate the law of the repo and turn a
diagnostic into a hazard. Gated proposals keep B useful (it drafts the fix) without
making it dangerous (a human owns the apply).

---

## Decision 6 — stub-LLM testing: prove the wiring offline, gate intelligence behind the live scorecard

Verification is split into two layers with an explicit honesty contract, because
"the plumbing is correct" and "the agent is correct" are different claims.

- **Offline (default suite):** the crew machinery runs with a scripted `StubLLM`
  (a `crewai.LLM` subclass; `create_llm` returns an `isinstance(x, LLM)` object
  as-is, so the real agent executor and `output_pydantic=Diagnosis` coercion run
  without token calls). `tests/sentinel/test_e2e_diagnose.py` injects each of the
  14 failures into the **live** I4 ledger and grades the typed-`Diagnosis` seam
  against the **live** oracle. A green run proves the loop is wired — injected
  incident → ledger row → typed Diagnosis → oracle match — and a negative-control
  case proves the oracle grades rather than rubber-stamps. The cascade path runs
  the real deterministic Flow against the live warehouse with no key.
- **Live (opt-in, key-gated):** `tests/sentinel/test_eval_live.py` is the only
  layer that touches a live LLM. It runs the full loop against the real crew and
  **records** the `ScoreResult` — asserting only that the loop completed, never
  that the score is "correct." It skips with a clear reason when no key is present.

**Why:** crew intelligence is exactly the probabilistic thing R5 forbids
asserting. Stubbing the token call makes the loop deterministic and offline, so CI
gets a real "is the plumbing correct" signal every run; the live scorecard is
where the honest, reported "is the agent any good" answer is produced — manually,
with a key — and never green-washed into the default suite.

---

## Consequences

**Positive:**
- The full inject→detect→score loop is wired and provable offline against the live
  I4 ledger for all 14 failures (R6), with the LLM intelligence question honestly
  isolated behind a key-gated scorecard (R5).
- The crew is read-only over the backbone (R3): every I1–I5 read is read-only, and
  the only writers touch `sentinel/` alone. A destructive fix cannot be applied
  without a human (HITL).
- Scoring is partial-credit and auditable, so a probabilistic crew's quality is
  *measured* (alias 0.7, cascade base + overlap) instead of collapsed to pass/fail.
- The poll-the-ledger trigger sees every injected incident, not just the ~2 that
  crash a run, and makes the loop a reproducible pure function of ledger state (R7).

**Negative / trade-offs:**
- Memory (`recurring_incident`) and Knowledge/RAG (`ambiguous_anomaly`) are
  **opt-in**: attaching an embedder/knowledge source needs an API key, so the
  default offline build does not exercise them and they are not covered by the
  green default suite — only by a keyed run.
- The cascade e2e case against a frozen warehouse scores `~0.667`, not 1.0:
  freshly injected Postgres defects have not flowed through dbt, so the Flow sees
  only the current silver state. Full-overlap 1.0 is proven deterministically with
  fixture tools, not against a live unrefreshed warehouse — a real 1.0 needs a
  heavy dbt run first.
- `volume_spike` detection is statistical and carries a documented caveat: the
  deterministic seeder bulk-loads its baseline within one minute, so a clean spike
  signal requires a reseeded baseline. The tool surfaces the raw numbers rather
  than a brittle boolean.
- Polling (vs a webhook) means detection latency is bounded by the poll cadence,
  not instantaneous — acceptable because the loop is an offline eval, not a
  real-time pager.

**Neutral:**
- A1 is a `manager_agent`, deliberately outside the `@agent` roster (CrewAI injects
  it). Intentional per the 0.100.0 hierarchical contract, not an oversight.
- The CrewAI 0.100.0 API specifics are pinned by version (singular `guardrail=` +
  `max_retries`; `@router` single-output; Agent has no `memory` field — memory is a
  crew capability). A version bump must re-verify these against the installed
  package before relying on newer-API behaviour.

## Citations

- Project handbook: [`../../CLAUDE.md`](../../CLAUDE.md) (component split, the agent fleet, the AC table)
- Operating rules: [`../../.claude/rules/agent-operating-rules.md`](../../.claude/rules/agent-operating-rules.md) (R3 one-way, R5 verify-by-scoring, R6 all-14, R7 reversibility, R8 secrets/SQL)
- Plan: [`../../sketch/sentinel-engine.md`](../../sketch/sentinel-engine.md) (the I1–I5 interface table, the remediation loop, build order)
- Code — Decision 1: `sentinel/crew.py` (`manager_agent_instance`, the `@agent`/`@crew` wiring), `sentinel/config/agents.yaml`
- Code — Decision 2: `sentinel/scoring.py` (`score_run`, the rubric constants, `ALIAS_MAP`, `EXPECTED_SURFACE`), `sentinel/models.py`
- Code — Decision 3: `sentinel/flow.py` (`SentinelFlow`, `diagnose_cascade`, the `@router`/`or_` routing)
- Code — Decision 4: `sentinel/trigger.py` (`poll_once`, `dispatch`, `run_sentinel`)
- Code — Decision 5: `sentinel/tools/resolution.py` (`ProposePatch`, `WritePostmortem`), `sentinel/config/tasks.yaml` (`propose_fix_task` HITL)
- Code — Decision 6: `tests/sentinel/test_e2e_diagnose.py`, `tests/sentinel/test_eval_live.py`, `tests/sentinel/stub_llm.py`
- Failure→capability map: `src/gen/failures.py` (`REGISTRY`, the `unlocks` fields)
