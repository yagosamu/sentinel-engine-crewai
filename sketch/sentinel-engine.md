# Sketch · Sentinel Engine

The **autonomous DataOps engine** — Layer 6. A CrewAI hierarchical crew that
watches the analytical backbone, diagnoses what breaks, and proposes the fix.
Probabilistic, not deterministic: its output is *judged* against ground truth
(did it diagnose the failure that was actually injected?), not asserted.

This is **Component B** of the two-component split. It is parasitic on
Component A (`analytical-backbone.md`): it consumes A's exhaust read-only and
delivers nothing without A underneath it. The dependency is one-directional.

> Plan altitude: agents, remediation loop, dependencies, build order, and the
> interface consumed from the backbone. No atomic tasks, no code.
>
> ⚠ **Scope flag:** the brief scopes "autonomous / agentic incident remediation"
> **out** ("an ops-automation problem with its own risk profile — evaluate after
> the foundation is proven"). Component B is in this program only if Phase 0
> keeps it. See backbone **U1**. This sketch plans the end-state; the brief
> decides when it's built.

---

## Components (the crew)

CrewAI **hierarchical process**: a Manager delegates to two squads. Model tiers
per the spec — Manager on the stronger tier, specialists on the cheaper one.

### A1 · Manager (Tech Lead) — hierarchical coordinator

Owns the investigation. Receives the failure trigger, delegates to the squads,
validates findings, approves the post-mortem.

- **Tools:** AssignTask, ReviewOutput.
- **Depends on:** all other agents (it orchestrates them).
- **Detects:** `multi_failure_cascade` (routes each sub-failure to the right squad).

### Investigation Squad

#### A2 · Log Analyst

Parses Dagster and dbt logs to pinpoint the broken node and error trace.

- **Tools:** ReadDagsterLogs, ReadDbtRunResults.
- **Consumes:** Dagster logs (I1), dbt run results (I2).
- **Detects:** `schema_drift`, `slow_source` — failures that surface as *pipeline*
  errors, not data values.

#### A3 · Data Profiler

Queries DuckDB to confirm the anomaly — missing column, statistical drift,
constraint violation — correlating it to the failure.

- **Tools:** QueryDuckDB, ProfileTable.
- **Consumes:** DuckDB `gold_`/`silver_` tables (I3).
- **Detects:** the data-quality failures (negative price, null customer, invalid
  quantity, duplicate, late arrival, volume spike, orphan payment, + advanced).

### Resolution Squad

#### A4 · Data Engineer

Writes the fix — the dbt migration or Dagster patch — for the diagnosed root cause.

- **Tools:** WriteCodePatch, ValidateDbtModel.
- **Consumes:** A2 + A3 diagnosis; the backbone's dbt/Dagster project.
- **Writes back:** a *proposed* patch — gated, never auto-applied (the weak B→A
  link; riskiest seam, built last).

#### A5 · Incident Commander

Searches the historical incident RAG for similar past issues; authors a
blameless post-mortem.

- **Tools:** QueryHistoricalRAG, WriteMarkdownReport.
- **Consumes:** the incident RAG (I5); the full investigation thread.
- **Writes:** the post-mortem (the crew's primary human-facing output).

---

## Failure → capability map

The crew must handle the **14 failures the generator already injects**, not the 4
the spec names. Six demand specific CrewAI features — the mapping lives in the
code's `unlocks` fields (`src/gen/failures.py`) and is the real design driver.

| Failure(s) | Detected by | CrewAI capability it forces |
| --- | --- | --- |
| negative_price, missing_customer, invalid_quantity, duplicate_order, late_arrival, volume_spike, orphan_payment | A3 Profiler | base crew (profiling tools) |
| schema_drift | A2 Log Analyst | base crew (log tools) |
| recurring_incident | A3 | **Memory** — recognise a repeat offender, don't cold-start |
| ambiguous_anomaly | A3 | **Knowledge/RAG** — runbook to pick between competing root causes |
| destructive_fix | A3 | **Human-in-the-loop** — destructive remediation pauses for approval |
| malformed_data | A3 | **Guardrails + output_pydantic** — typed, validated post-mortem |
| slow_source | A2 | **Tool reliability** — max_retry, timeouts, fallbacks on flaky tools |
| multi_failure_cascade | A1 Manager | **Flows + conditional routing** — fan failures to the right squad |

> Implication: the base crew handles ~9 failures; the last 5 each unlock one
> feature. Build the crew **feature-by-feature against this taxonomy**, not all
> at once.

---

## The remediation loop (build target)

```text
Detect ──► Trigger ──► Investigate ──────► Resolve ──────► Score
generator   webhook     Manager assigns:    Manager assigns:  diagnosis vs.
injects     fires to     A2 Log Analyst      A4 Data Eng (fix) injected_incidents
failure     Manager      A3 Data Profiler    A5 Commander      ground truth (I4)
            (B1)                              (post-mortem)
```

### B1 · Trigger — polls the ledger, not a pipeline-failure webhook

Bridges A's failure to B's Manager.

- **Does:** polls the `injected_incidents` ledger (I4) for rows since a cursor and
  invokes the crew. **This is a deliberate departure from the tech-spec's §6.3
  webhook design, not an oversight** (Codex's adversarial pass flagged it as
  drift — it's real drift from the spec text, but the spec's own framing in §3
  supports the fix: the Data Generator is "a permanent testing harness," and
  only ~2 of the 14 injected failures actually crash a Dagster run — the other 12
  land quietly in `silver_*_rejects` (quarantine-not-drop, U3). A run-failure
  webhook would never fire for them. Polling the same ledger the scoring oracle
  reads keeps trigger and oracle consistent.
- **Named limit, accepted:** this trigger only works because I4 exists — i.e.
  it's validation-harness-specific. A production deployment with no synthetic
  ground-truth ledger would need a real observability-based trigger (log/metric
  anomaly detection), which is out of scope here and not designed. Don't mistake
  "detects injected incidents" for "detects production incidents" — the plan
  detects the label, not an independently-observed symptom, by design, for now.
- **Depends on:** C1 (ledger). Not C2 run status — see above.

### B2 · Incident RAG

Historical post-mortems for A5 to consult.

- **Does:** stores past incidents; serves similarity search.
- **Cold-start gap (named by Codex Step 1, accepted):** `injected_incidents` rows
  are thin structured signal (failure_key, timestamp, payload) — they are not
  post-mortem narratives, so bootstrapping directly from I4 gives A5 something to
  query but not much to *reason* over. Meaningful similarity search only exists
  once A5 has actually run and written a few real postmortems into
  `sentinel/postmortems/`. **Accepted**: early runs get thin RAG context; this
  degrades gracefully (A5 still produces a structured report, just without a rich
  "seen this before" signal) rather than failing.
- **Depends on:** A5 output accumulating over time.

---

## Interface needed FROM the backbone (the seam)

**Read-only contract. A produces, B observes.** This table is the entire surface
between the two plans — if these are stable, A and B build as separate tracks.

| ID | Surface | From | Used by |
| --- | --- | --- | --- |
| I1 | Dagster run logs / asset status | C2 Ingestion | A2, B1 |
| I2 | dbt run results | C3 Transform | A2 |
| I3 | DuckDB `gold_` / `silver_` tables | C4 Warehouse | A3 |
| I4 | `injected_incidents` + failure signature | C1 Source | scoring, B1 |
| I5 | Incident RAG store | B2 (self) | A5 |

I4 is the **load-bearing seam** — the ground-truth oracle that makes B scorable.
It already exists in C1 today. **I4 is dual-use and that's worth naming
explicitly** (Codex flagged the ambiguity): it is both what B1 polls to *trigger*
a run and what the scorer reads to *grade* the run afterward. Scoring against I4
is the whole point (R5) and not in question. Triggering off I4 is the
validation-harness-specific choice explained under B1 above — accepted for this
program, not assumed to carry over to a production deployment unchanged.

---

## Dependencies & build order

```text
A2 Log Analyst ┐
A3 Data Profiler ┘─► A1 Manager ─► A4 Data Engineer ─► A5 Incident Commander
(build + test each agent against ONE injected failure, then wire the loop)
```

B cannot be meaningfully tested until A's interface surfaces exist and emit on
failure: **C1 (have it) + C2 + C3 + C4 must be live first.**

**Phase 3 — autonomous ops** (after backbone Phase 1 only — Sentinel's own
interface table above needs C1–C4, not C5. The original text said "Phases 1–2,"
which wrongly ties B's start to C5/MCP, an optional component per U1. Corrected
here per Codex Step 2. Brief defers this phase regardless — see the scope flag
at the top of this file):

1. **B1 trigger + A1 Manager + base A2/A3** — detect & diagnose the ~9 base
   failures end-to-end against I4 ground truth. (Investigation agents first —
   read-only, easiest to verify; then the Manager to orchestrate.)
2. **A4 Data Engineer + A5 Incident Commander** — close the loop (propose fix,
   write post-mortem). A4's write-back stays gated.
3. **Feature unlocks**, one failure at a time: Memory → RAG (needs B2) → HITL →
   Guardrails → tool-reliability → Flows/cascade routing.

---

## Open / unresolved

*Sharpened in Pass 4 (Codex adversarial review, 2026-08-18).*

- **U3 · Detection seam — FIXED, resolved.** ADR-0001 Decision 2: quarantine, not
  drop. Every defect lands in `silver_<entity>_rejects` (tagged with
  `reject_rule == failure_key`), absent from gold. That's the concrete,
  per-defect answer — not open. (One documented exception, itself resolved:
  `ambiguous_anomaly` has no deterministic surface by design — it's a
  Knowledge/RAG-only failure, scored on the Tier-1 key match with `evidence=0.0`,
  never on a fabricated I3 probe.)
- **Scoring rubric — FIXED, resolved.** ADR-0002 Decision 2: a two-tier rubric,
  not a single exact-match boolean. Tier 1 (gating): 1.0 exact, 0.7 for the one
  registry-justified alias (`recurring_incident ↔ negative_price`), 0.5 + 0.5×
  overlap for a cascade, 0.0 miss. Tier 2 (non-gating): did the diagnosis cite the
  right evidence surface, catching a lucky guess.
- **Reset-to-clean for reproducible evals — FIXED, resolved (R7).** The trigger
  captures a `since` cursor *before* inject, so the scored window holds exactly
  that case's ledger rows and the run is reproducible.
- **Trigger mechanism (B1) — FIXED, resolved, and named as an accepted limit.**
  Polling I4, not a Dagster-failure webhook — see B1 above for why, and for the
  accepted validation-harness-only caveat.
- **Model tiering — FIXED, matches the spec.** `sentinel/config/agents.yaml`:
  manager on `gpt-4o`, all four specialists on `gpt-4o-mini` — exactly the tech
  spec's tiering (§6.1). The original open item asked about a "per-incident cost
  target"; the spec only gives a *monthly* target (§8.1, "under $50/month for
  typical monitoring workloads"), not a per-incident one — that was a
  mis-specified question, corrected here, not an open decision.
- **Human-gating vs. the spec's "without human intervention" language —
  ACCEPTED, intentional divergence.** The tech-spec (§6.3, §9) describes the
  Sentinel proposing fixes "without human intervention." The build requires
  human approval before A4 even emits a proposal for `destructive_fix`
  (HITL, `human_input=True`). This is a deliberate safety call, not spec
  drift by accident: the brief scopes "autonomous / agentic incident
  remediation" out as its own initiative with its own risk profile, and a
  bulk-overwrite proposal is exactly the kind of action that justifies a human
  in the loop. Accepted as correct; the spec's framing is the thing that's
  wrong here, not the plan. *(Owner: CTO — reaffirm if agentic autonomy is ever
  formally brought in-scope.)*
