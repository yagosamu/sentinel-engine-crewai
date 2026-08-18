# sentinel/ — Component B: the Sentinel engine

The probabilistic half of the system. Component A (the analytical backbone,
[`../platform`](../platform)) lifts an e-commerce source through a medallion into
a DuckDB warehouse; the chaos generator ([`../src/gen`](../src/gen)) injects 14
failure modes into the source and logs each as ground truth in an
`injected_incidents` ledger. `sentinel/` is the autonomous DataOps crew that
**watches A, diagnoses those injected failures, and proposes fixes** — and is
**scored against that ledger**, never asserted correct.

> This README is the contributor handbook for `sentinel/`. Project-wide context
> (the two-component split, the agent fleet, the operating rules, the open
> decisions) lives in [`../CLAUDE.md`](../CLAUDE.md) and
> [`../.claude/rules/agent-operating-rules.md`](../.claude/rules/agent-operating-rules.md);
> it is referenced here, not duplicated. The plan is
> [`../sketch/sentinel-engine.md`](../sketch/sentinel-engine.md); the locked
> decisions are recorded in
> [`../docs/adrs/0002-sentinel-engine.md`](../docs/adrs/0002-sentinel-engine.md).

---

## WHAT — a five-agent hierarchical crew

`sentinel/` is one CrewAI **hierarchical** crew plus the trigger that feeds it,
the Flow that handles the one multi-dimensional incident, and the oracle that
grades the result. A1 (the manager) delegates to two squads:

| Agent | Role | Tools | Surface |
|-------|------|-------|---------|
| **A1 Manager** | Tech Lead — the hierarchical coordinator. Delegates, never detects. | none (delegation is structural in `Process.hierarchical`) | — |
| **A2 Log Analyst** | Detects *pipeline-level* failures. | `ReadDagsterLogs`, `ReadDbtRunResults` | I1, I2 |
| **A3 Data Profiler** | Confirms *data-quality* defects. | `ProfileRejects`, `QueryDuckDB` | I3 |
| **A4 Data Engineer** | Drafts a **gated, advisory** proposed patch. | `ProposePatch` (writes `sentinel/proposed/` only) | — (writes B-only) |
| **A5 Incident Commander** | Recalls prior incidents; writes the typed post-mortem. | `QueryIncidentRAG`, `WritePostmortem` | I5 |

A1 is a **custom `manager_agent`**, not a member of the specialist roster: it is
constructed by `SentinelCrew.manager_agent_instance()` and passed via
`manager_agent=` (CrewAI injects it into the run), so it never appears in
`agents=[...]`. The specialists (A2–A5) have `allow_delegation=False`; only the
manager delegates. See [`crew.py`](crew.py).

The investigation squad's `synthesize_diagnosis_task` emits a typed
[`Diagnosis`](models.py) (`output_pydantic=Diagnosis`). That typed object — its
`failure_key`, `sub_failures`, and `evidence_surface` — is the load-bearing seam
the scoring oracle consumes. Keeping it typed (not free text) is what makes
Component B *scorable* instead of merely asserted (R5).

---

## The 14-failure → capability map

The crew must handle **all 14** generator failures (R6), not the four the tech
spec names. Each advanced failure forces a specific CrewAI feature. The map below
is grounded in [`../sketch/sentinel-engine.md`](../sketch/sentinel-engine.md) and
the `unlocks` field on each failure in
[`../src/gen/failures.py`](../src/gen/failures.py).

| # | failure_key | Detected by | Evidence surface | CrewAI capability it unlocks |
|---|-------------|-------------|------------------|------------------------------|
| 1 | `negative_price` | A3 Data Profiler | `silver_orders_rejects` | base crew |
| 2 | `missing_customer` | A3 | `silver_orders_rejects` | base crew (null/constraint) |
| 3 | `invalid_quantity` | A3 | `silver_orders_rejects` | base crew (range/domain) |
| 4 | `duplicate_order` | A3 | `silver_orders_rejects` | base crew (uniqueness) |
| 5 | `orphan_payment` | A3 | `silver_payments_rejects` | base crew (referential) |
| 6 | `late_arrival` | A3 | `silver_orders.is_late` | base crew (freshness) |
| 7 | `volume_spike` | A3 | `silver_orders` count | base crew (statistical) |
| 8 | `schema_drift` | A2 Log Analyst | `silver_orders._schema_drift` + dbt error | base crew (Log Analyst tools) |
| 9 | `slow_source` | A2 | Dagster logs (flaky read) | **Tool reliability** — A2 `max_retry_limit` + per-call tool timeout |
| 10 | `recurring_incident` | A3 | `silver_orders_rejects` (negative prices) | **Memory** — `Crew(memory=True)` (opt-in) |
| 11 | `ambiguous_anomaly` | A1 + Knowledge (no A3 probe) | none deterministic — LLM reasons over `silver_orders`/`silver_products` counts | **Knowledge / RAG** — runbook `knowledge_sources` (opt-in) |
| 12 | `destructive_fix` | A3 | `silver_orders_rejects` | **HITL** — `propose_fix_task.human_input=True` |
| 13 | `malformed_data` | A3 | `silver_orders_rejects` | **Guardrails + `output_pydantic`** — typed, validated post-mortem |
| 14 | `multi_failure_cascade` | A1 Manager | several surfaces at once | **Flows + conditional routing** — [`flow.py`](flow.py) |

Items 1–8 are the base hierarchical crew. Items 9–13 are advanced unlocks wired
into [`crew.py`](crew.py) (Memory and Knowledge are **opt-in** because attaching
an embedder needs an API key — the default build stays offline-safe). Item 14 is
handled by a dedicated Flow, not the crew (see HOW below).

**How a key is detected (and what is deliberately *not* in the deterministic
maps).** A reader auditing the two map files will find them intentionally
asymmetric — here is the full account so the asymmetry reads as design, not a hole:

- **Rejects-table failures** (`negative_price`, `missing_customer`,
  `invalid_quantity`, `duplicate_order`, `malformed_data`, `destructive_fix`,
  `recurring_incident`, `orphan_payment`) **and accepted-flag/statistical
  failures** (`late_arrival`, `schema_drift`, `volume_spike`) each have a
  deterministic A3 probe in `DETECTION_MAP` ([`tools/warehouse.py`](tools/warehouse.py),
  11 keys) — A3 confirms them by querying the warehouse, no LLM required.
- **A2 log-surface failures** (`slow_source`, and `schema_drift` as a secondary
  log signal) are detected in the *logs* (I1/I2), not the warehouse. `slow_source`
  therefore has **no `DETECTION_MAP` entry by design** — `ProfileRejects` returns
  "no I3 detection probe" for it — but it *does* carry an `EXPECTED_SURFACE` entry
  (`dagster_logs`) in [`scoring.py`](scoring.py) so its evidence is still gradable.
  This is why the two maps do not line up key-for-key: a log-surface failure lives
  in the scoring surface map but not the warehouse probe map.
- **`ambiguous_anomaly` is Knowledge/RAG-only.** It has **no `DETECTION_MAP`
  probe and no `EXPECTED_SURFACE` entry** — it is resolved by the manager reasoning
  over the disambiguation [`knowledge/runbook.md`](knowledge/runbook.md) (the crew
  can `QueryDuckDB`-count the allow-listed `silver.silver_orders` /
  `silver.silver_products`, but there is no deterministic detection rule and no
  evidence-surface credit). Known scoring limitation: a *correct* `ambiguous_anomaly`
  diagnosis scores `evidence=0.0` because no expected surface is registered — its
  Tier-1 key match still scores `1.0`. This distinguishes it from the rejects-table
  failures above, which earn evidence credit.
- **`multi_failure_cascade` is Flow-only.** It is absent from both maps by design:
  it has its own deterministic Flow ([`flow.py`](flow.py)) and its own cascade
  scoring tier (base `0.5` + member overlap), not a single-key probe or surface.

---

## The interface it reads (I1–I5)

Component B consumes Component A **read-only** through five surfaces. The
dependency is one-directional: **A produces, B observes** (R3). A never imports,
calls, or waits on B; B pulls A's exhaust. The full contract is in the sketch
([interface table](../sketch/sentinel-engine.md)); the surfaces as actually wired:

| ID | Surface | Source | Read by | How (tool / path) |
|----|---------|--------|---------|-------------------|
| **I1** | Dagster run logs / asset status | C2 Ingestion | A2, B1 | `ReadDagsterLogs` scans `DAGSTER_HOME` for the `BACKBONE_RUN_FAILURE` contract line ([`tools/logs.py`](tools/logs.py)) |
| **I2** | dbt run results | C3 Transform | A2 | `ReadDbtRunResults` parses `platform/transform/target/run_results.json` ([`tools/logs.py`](tools/logs.py)) |
| **I3** | DuckDB `silver_`/`gold_` tables | C4 Warehouse | A3 | `ProfileRejects`, `QueryDuckDB` open the warehouse via `connect_read_only` (physically cannot write) ([`tools/warehouse.py`](tools/warehouse.py)) |
| **I4** | `injected_incidents` ledger | C1 Source | scoring, B1 | the trigger and the oracle read the SAME ledger via `src.gen.repository.session` ([`trigger.py`](trigger.py), [`scoring.py`](scoring.py)) |
| **I5** | Incident RAG store | B2 (self) | A5 | `QueryIncidentRAG` bootstraps from I4, accumulates as A5 writes post-mortems ([`tools/rag.py`](tools/rag.py)) |

I4 is the **load-bearing seam** — the ground-truth oracle that makes B scorable.
It already exists in C1 today (the generator writes it).

The two tools that *write* — `ProposePatch` (A4) and `WritePostmortem` (A5) —
write **only** under the `sentinel/` tree (`proposed/`, `postmortems/`). They
never edit `platform/`. The output path is slugified and re-checked with
`relative_to`, so a crafted `failure_key` cannot traverse out of the sentinel
directory. The proposed patch is the only B→A link, and it is gated: advisory,
never auto-applied — a human reviews and applies it by hand, if at all (R3).

---

## HOW — running the Sentinel

All commands are from the repo root, with `PYTHONPATH=$(pwd)` so the local
packages resolve. The trigger and oracle need the **live Postgres source + I4
ledger** (`make up`); the A3 warehouse reads need the **DuckDB warehouse**
populated by a backbone run. Running the *crew* additionally needs an **LLM API
key**; the deterministic cascade Flow and the scoring oracle do not.

### 1. The B1 trigger — poll, dispatch, score

The trigger **polls** the `injected_incidents` ledger (I4) rather than hooking a
Dagster failure webhook: only ~2 of the 14 failures actually crash a Dagster run;
the other ~12 are quarantined rows in `silver_*_rejects` (the backbone run
*succeeds* — that is the point of quarantine-not-drop), so a failure webhook would
never fire for them. Polling the same table the scorer reads keeps trigger and
oracle consistent. See [`trigger.py`](trigger.py).

```bash
# Poll the live ledger for incidents injected since a cursor, dispatch, print the score.
# Capture a cursor just BEFORE inject so the inject->detect->score window is reproducible (R7).
uv run python -m sentinel.trigger --since "2026-06-30T00:00:00+00:00"

# Default (--since now) reports a no-run: nothing has been injected since this instant.
uv run python -m sentinel.trigger
```

Dispatch logic ([`trigger.py`](trigger.py) `dispatch`):
- **single** distinct `failure_key` in the window → the base hierarchical crew
  ([`SentinelCrew`](crew.py)). A destructive failure flips `human_gated_fix=True`
  so A4's propose task pauses for approval.
- **several** distinct keys in one window → a `multi_failure_cascade`, routed to
  the deterministic cascade Flow (the cascade injector writes one ledger row per
  member and no top-level row, so the trigger recognises a cascade by simultaneous
  arrival of distinct members).
- crew could not run (no API key, embedder error, crash) → an honest **no-run**
  (`diagnosis=None`), never a fabricated score.

### 2. The cascade Flow — `multi_failure_cascade`

The one failure a single `failure_key` cannot represent. The generator fires
`missing_customer` + `volume_spike` + `schema_drift` together; the Flow
([`flow.py`](flow.py)) triages each member against its real evidence surface
(I2/I3) **deterministically — no LLM**, routes pipeline vs data members to
chained squad branches, and synthesizes a `Diagnosis` carrying
`failure_key="multi_failure_cascade"` plus the detected `sub_failures`. Because
triage reads real evidence rather than calling an LLM, the routing/fan-out wiring
is verifiable offline.

```python
from sentinel.flow import diagnose_cascade
diagnosis = diagnose_cascade()   # real Flow, live read-only warehouse read, no API key
```

Everything else stays on the plain hierarchical crew — Flow-ifying the 13
single-failure paths would add state and latency for no gain.

### 3. Scoring against I4 — the oracle

[`scoring.py`](scoring.py) reads the `injected_incidents` ledger (I4) and grades a
`Diagnosis` against it. It **scores against ground truth; it never asserts the
crew is correct** (R5). A run the crew could not complete returns a NO-RUN result,
never a fabricated score. The rubric is deterministic and auditable (every
threshold is a module constant):

- **Tier 1 — diagnosis match** (gating, 0.0–1.0):
  - `1.0` EXACT — `diagnosis.failure_key == oracle.failure_key`.
  - `0.7` ALIAS — a registry-justified equivalence. The only alias is
    `recurring_incident ↔ negative_price` (the recurring injector writes negative
    prices). The alias map is a tiny explicit dict, never fuzzy text.
  - `0.5 + 0.5 × (named ∩ actual / actual)` CASCADE-PARTIAL for
    `multi_failure_cascade`.
  - `0.0` MISS.
- **Tier 2 — evidence quality** (non-gating, reported separately): did the
  diagnosis cite the correct I-surface? This catches "right key, fabricated
  reasoning" — a lucky guess scores `match=1.0 evidence=0.0`, visibly flagged, not
  rewarded.

The legacy `score_diagnosis(key) -> "correct"|"incorrect"` API is kept as a thin
shim over `score_run` so existing callers and tests stay valid.

### 4. The e2e proof and the live scorecard — stub vs live (R5)

The verification has two distinct layers, with an explicit honesty contract:

- **`tests/sentinel/test_e2e_diagnose.py` — the offline inject→detect→score
  proof.** For every one of the 14 failures, it injects a real row into the live
  ledger and grades the crew's typed-`Diagnosis` seam against the live I4 oracle.
  The token-producing LLM call is **stubbed** (`StubLLM` scripted to emit the
  correct key); everything else is live — the inject, the I4 ledger contract, the
  `output_pydantic=Diagnosis` coercion exactly as the crew wires it, the cascade
  Flow against the live warehouse, and the oracle. A green run proves **the
  plumbing is correct** — that an injected incident becomes a ledger row, the typed
  Diagnosis reaches the oracle, and the oracle grades it against ground truth. It
  does **not** prove an LLM is smart enough to diagnose unaided. A negative-control
  case asserts a wrong diagnosis scores a MISS, so the oracle is shown to grade,
  not rubber-stamp.

  ```bash
  PYTHONPATH=$(pwd) \
  DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb \
    uv run pytest tests/sentinel/test_e2e_diagnose.py
  # Skipped (never faked) with a clear reason if Postgres is down.
  ```

- **`tests/sentinel/test_eval_live.py` — the API-key-gated live scorecard.** The
  only layer that touches a live LLM. It runs the full loop against the *real*
  crew and **records** the `ScoreResult` — it asserts the loop completed and
  produced a score; it does **not** assert the score is "correct" (a crew that
  scores 0.4 on `ambiguous_anomaly` is a real, honest result — that is what
  scoring is *for*). Skips with a clear reason when no key is present.

  ```bash
  OPENAI_API_KEY=... PYTHONPATH=$(pwd) \
  DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb \
    uv run pytest tests/sentinel/test_eval_live.py -s   # -s to see the [SCORECARD] lines
  ```

The suite is `tests/sentinel/` — **68 test functions across 10 files**
(`test_crew_build` 14, `test_tools` 13, `test_trigger` 9, `test_rubric` 8,
`test_flow` 6, `test_e2e_diagnose` 6, `test_crew_stub` 4, `test_rag` 4,
`test_scoring` 3, `test_eval_live` 1). Two of those functions are parametrized,
so the *collected* case count is higher than 68: `test_e2e_diagnose` fans out
over the single-failure registry, and `test_eval_live` fans out over the six
`LIVE_SCOREABLE_FAILURES`. Offline (no API key), the **six key-gated live
scorecard cases in `test_eval_live` skip** with a clear reason — they add no
green-suite signal and make no live claim; the `test_e2e_diagnose` cases skip
only if Postgres is unreachable. Run the suite for the exact passed/skipped
totals in your environment (they are not asserted here because that would be a
number this document did not observe).

> **Honesty — what is partial.** The cascade e2e case against a *frozen* warehouse
> scores `~0.667` (base 0.5 + member overlap), not a full 1.0: the warehouse is a
> clean snapshot, so freshly injected Postgres defects have not flowed through dbt
> yet and the Flow detects only what the current silver state shows. The case
> asserts the structural ground truth (cascade routing + oracle wiring) and
> *reports* the member overlap rather than demanding a brittle full-overlap that
> needs a heavy dbt run. Full-overlap 1.0 is proven deterministically in a
> sibling case using fixture tools. `volume_spike` detection carries a documented
> caveat (the seeder bulk-loads its baseline in one minute, so a clean spike
> signal needs a reseeded baseline — [`tools/warehouse.py`](tools/warehouse.py)).

---

## WHERE — file map of `sentinel/`

```text
sentinel/
├── crew.py            SentinelCrew — the 5-agent hierarchical crew (A1 manager + A2-A5).
│                      Wires the advanced unlocks (Memory, Knowledge, HITL, Guardrails, tool retry).
├── flow.py            SentinelFlow + diagnose_cascade() — the multi_failure_cascade Flow
│                      (deterministic, offline-safe conditional routing).
├── trigger.py         B1 entrypoint: poll_once() polls I4, dispatch() routes base/cascade/no-run,
│                      run_sentinel() ties it together. `python -m sentinel.trigger` CLI.
├── scoring.py         The I4 ground-truth oracle: score_run (evidence + cascade rubric),
│                      score_diagnosis (legacy exact-match shim). Reports, never asserts (R5).
├── models.py          Typed crew->scoring contracts: Diagnosis, Postmortem.
├── guardrails.py      validate_postmortem — the malformed_data guardrail (typed report from garbage).
├── tools/
│   ├── logs.py        A2: ReadDagsterLogs (I1), ReadDbtRunResults (I2). Read-only.
│   ├── warehouse.py   A3: ProfileRejects, QueryDuckDB (I3). Read-only (connect_read_only).
│   ├── rag.py         A5: QueryIncidentRAG (I5). Read-only; bootstraps from I4.
│   └── resolution.py  A4: ProposePatch (gated, sentinel/proposed/ only);
│                      A5: WritePostmortem (sentinel/postmortems/). The only writers.
├── config/
│   ├── agents.yaml    A1-A5 roster: role/goal/backstory + tiered model names (manager stronger).
│   └── tasks.yaml     The task chain: analyze_logs -> profile -> synthesize -> propose -> postmortem.
├── knowledge/
│   └── runbook.md     The disambiguation runbook (the ambiguous_anomaly Knowledge source).
├── proposed/          A4 output: gated, advisory patches (never applied to platform/).
└── postmortems/       A5 output: blameless typed post-mortems.
```

`proposed/` and `postmortems/` are **runtime-populated** — in a fresh checkout
they hold only a `.gitkeep` and are otherwise empty. They fill only when the crew
actually runs with a live API key against a live warehouse (`A4 ProposePatch` /
`A5 WritePostmortem` write the first files); the offline default build and the
stub-LLM tests do not populate them.

Tests live in [`../tests/sentinel/`](../tests/sentinel): `test_scoring.py` /
`test_rubric.py` (oracle), `test_tools.py` / `test_rag.py` (tools),
`test_crew_build.py` / `test_crew_stub.py` (crew assembly + unlocks),
`test_flow.py` (cascade routing), `test_trigger.py` (dispatch),
`test_e2e_diagnose.py` (the offline proof), `test_eval_live.py` (the gated
scorecard). `stub_llm.py` is the scripted `crewai.LLM` subclass that makes the
crew machinery run offline.

---

## Conventions specific to this package

These extend (don't replace) the repo-wide rules in
[`../.claude/rules/agent-operating-rules.md`](../.claude/rules/agent-operating-rules.md):

- **Score, never assert (R5).** A Sentinel result is *graded* against the I4
  oracle. Never assert the crew is "correct"; report the `ScoreResult`. A run that
  could not complete is a NO-RUN, never a fabricated score.
- **One-way seam (R3).** Every I1–I5 read is read-only. The two writers touch only
  `sentinel/`. Do not introduce a coupling that makes A import, call, or wait on B.
- **Offline-safe by default.** The default crew build never touches an embedder;
  Memory and Knowledge are opt-in because they need an API key. The cascade Flow
  and the oracle are fully deterministic and run with no key. When no key is
  present, the crew kickoff is captured as a no-run — not a crash, not a fake score.
- **All 14 or it is not done (R6).** The crew handles every generator failure. The
  `test_every_registry_failure_is_covered` meta-test fails if a new failure escapes
  the e2e scorecard.
- **Reversible eval (R7).** Each inject→detect→score run reverts schema drift and
  captures a `since` cursor first, so the scored window holds exactly that case's
  ledger rows and the loop is reproducible.

## Tests

```bash
uv run ruff check sentinel tests/sentinel                 # lint
PYTHONPATH=$(pwd) DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb \
  uv run pytest tests/sentinel    # 68 functions; the 6 key-gated live cases skip offline
```

The DuckDB single-writer rule still applies: the cascade Flow and A3 tools open
read-only and serialize. A stale warehouse lock holder must be cleared before a
warehouse-touching run.
