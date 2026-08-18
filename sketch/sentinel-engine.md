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

### B1 · Trigger / webhook

Bridges A's failure to B's Manager.

- **Does:** detects a pipeline failure (Dagster run status) or polls for new
  `injected_incidents` rows; invokes the crew.
- **Depends on:** C2 (run status), C1 (ledger).

### B2 · Incident RAG

Historical post-mortems for A5 to consult.

- **Does:** stores past incidents; serves similarity search. `injected_incidents`
  can bootstrap it; cold-starts empty.
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
It already exists in C1 today.

---

## Dependencies & build order

```text
A2 Log Analyst ┐
A3 Data Profiler ┘─► A1 Manager ─► A4 Data Engineer ─► A5 Incident Commander
(build + test each agent against ONE injected failure, then wire the loop)
```

B cannot be meaningfully tested until A's interface surfaces exist and emit on
failure: **C1 (have it) + C2 + C3 + C4 must be live first.**

**Phase 3 — autonomous ops** (after backbone Phases 1–2; brief defers):

1. **B1 trigger + A1 Manager + base A2/A3** — detect & diagnose the ~9 base
   failures end-to-end against I4 ground truth. (Investigation agents first —
   read-only, easiest to verify; then the Manager to orchestrate.)
2. **A4 Data Engineer + A5 Incident Commander** — close the loop (propose fix,
   write post-mortem). A4's write-back stays gated.
3. **Feature unlocks**, one failure at a time: Memory → RAG (needs B2) → HITL →
   Guardrails → tool-reliability → Flows/cascade routing.

---

## Open / unresolved

- **U3 · Detection seam (the critical one).** Failures are injected into *raw
  Postgres*, often by dropping constraints; A3 is specced against *gold DuckDB*.
  If silver cleans/dedupes/types the data, the defect vanishes before A3 looks.
  **The interface contract (I3) must define which defects survive to which layer**
  — otherwise A3's detection tasks have no verifiable target. Blocks all of B's
  Profiler work. *(Resolve in the backbone C2/C3 spec before planning B.)*
- **Scoring rubric.** "Correct diagnosis" needs a concrete definition — exact
  `failure_key` match? root-cause text similarity? Determines how B is evaluated;
  decide before A1/A2/A3 tasks.
- **Reset-to-clean for reproducible evals.** Injectors drop constraints with no
  restore path. Every "inject → detect → score" run needs an explicit baseline
  restore, or B's evals are non-reproducible. *(Shared with backbone; affects C3
  and every B eval.)*
- **Trigger mechanism (B1):** webhook on Dagster run failure vs. polling the
  ledger on a schedule — decide at task time; affects detection latency.
- **Model tiering:** spec suggests a stronger model (Manager) + cheaper
  (specialists); confirm against the per-incident cost target.
