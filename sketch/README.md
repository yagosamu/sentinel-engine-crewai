# `sketch/` — the two plans

Plan-altitude design docs for this platform. **Two files, two components, one
split.** They describe *what gets built and in what order* — features,
boundaries, dependencies, acceptance-criteria mapping. **No atomic tasks, no
code.** Tasks come from `task-architect`; code lives in `platform/`, `sentinel/`,
and `src/`.

| File | Component | Layer(s) | Nature |
| --- | --- | --- | --- |
| [`analytical-backbone.md`](analytical-backbone.md) | **A — Analytical backbone** | 1–5 | deterministic (verified by assertion) |
| [`sentinel-engine.md`](sentinel-engine.md) | **B — Sentinel engine** | 6 | probabilistic (scored vs. ground truth) |

The dependency is **one-directional**: B reads A's exhaust read-only via the
interface contract (I1–I5); A never depends on B. This is rule **R3** — the law
of the repo (`.claude/rules/agent-operating-rules.md`).

---

## What the sketches are (and aren't)

- They are the **design source** the ADRs and task plans descend from. When a
  decision in a sketch is settled, it is *recorded in an ADR* — the sketch itself
  is not rewritten. Read sketches for the *plan*; read ADRs for the *resolution*.
- They are **stale on build status by design.** Each sketch was written before the
  layers were built and tags components `built` / `missing` / `to build` **as of
  authoring**. Those tags are now out of date — see the build-status pointer
  below. The *plan* (build order, dependencies, AC map, interface seam) remains
  the reference; the *status words* do not.

---

## The id vocabulary (C / A / I / U / AC)

Every component, agent, interface, decision, and gate carries a stable id. These
ids are the shared language across the sketches, `CLAUDE.md`, the ADRs, and the
tests. Glossary:

### `C#` — Components of Component A (the backbone)

| id | Component | Layer | Lives in |
| --- | --- | --- | --- |
| C1 | Source — Postgres + `injected_incidents` ledger | 1 | `src/db`, `src/seed`, `src/gen` |
| C2 | Ingestion — Dagster software-defined assets | 2 | `platform/ingestion/` |
| C3 | Transformation — dbt medallion (bronze/silver/gold) | 3 | `platform/transform/` |
| C4 | Warehouse — DuckDB | 4 | `platform/warehouse/` |
| C5 | Intelligence — FastAPI + MCP server | 5 | `platform/intelligence/` |
| C4h | Peak-load harness (spec-omitted, brief-required) | — | `platform/harness/` |
| C8 | Freshness probe (spec-omitted, brief-required) | — | `platform/probe/` |

> **There is no `C7`.** An earlier draft of `analytical-backbone.md` carried a
> stray `C7` in C4's "Depends on" note ("C5/C7/C8 read from"); it was never a
> declared component. It has been corrected to `C4h` (the peak-load harness is a
> warehouse reader). If you see `C7` referenced anywhere, it is a typo for `C4h`.

### `A#` — Agents of Component B (the Sentinel crew)

| id | Agent | Squad | Lives in |
| --- | --- | --- | --- |
| A1 | Manager (Tech Lead) — hierarchical coordinator | — | `sentinel/crew.py`, `sentinel/flow.py` |
| A2 | Log Analyst | Investigation | `sentinel/` |
| A3 | Data Profiler | Investigation | `sentinel/` |
| A4 | Data Engineer (proposes gated fix) | Resolution | `sentinel/` |
| A5 | Incident Commander (post-mortem) | Resolution | `sentinel/` |

### `B#` — Sentinel infrastructure (non-agent)

| id | Piece | Lives in |
| --- | --- | --- |
| B1 | Trigger / webhook — bridges A's failure to A1 Manager | `sentinel/trigger.py` |
| B2 | Incident RAG — historical post-mortems for A5 | `sentinel/knowledge/`, `sentinel/postmortems/` |

### `I#` — Interface seam (A produces, B observes; read-only contract)

| id | Surface | From | Used by |
| --- | --- | --- | --- |
| I1 | Dagster run logs / asset status | C2 | A2, B1 |
| I2 | dbt run results | C3 | A2 |
| I3 | DuckDB `gold_` / `silver_` tables | C4 | A3 |
| I4 | `injected_incidents` + failure signature (ground-truth oracle) | C1 | scoring, B1 |
| I5 | Incident RAG store | B2 | A5 |

I4 is the **load-bearing seam** — the only thing that makes B *scorable* rather
than merely asserted (rule R5). It already existed in C1 from day one.

### `U#` — Open decisions (gates; do not silently resolve — rule R4)

| id | Decision | Status |
| --- | --- | --- |
| U1 | Scope — is C5 (+ Component B) in this program, or "evaluate later"? | resolved → full scope incl. MCP (ADR-0001) |
| U2 | Raw boundary — Dagster (C2) vs. dbt bronze (C3) owns raw landing | resolved → Dagster owns raw (ADR-0001) |
| U3 | Detection seam — which defects survive bronze→silver→gold to where A3 looks | resolved → quarantine-not-drop (ADR-0001) |

> The sketches present U1–U3 as **open**. They were resolved after authoring and
> are recorded in **ADR-0001**; the resolutions also live in agent memory
> (`backbone-locked-decisions.md`). The sketches were not updated — read the ADR
> for the live answer.

### `AC-#` — Acceptance criteria (the brief's pass/fail gates)

| id | Proves | Covered by |
| --- | --- | --- |
| AC-1 | Peak isolation (75k orders/day, zero analytics lock-waits) — **go/no-go** | C4h |
| AC-2 | Query latency p95 ≤ 5s | C4, C3 (gold OBTs) |
| AC-3 | Freshness ≤ 5 min | C2 (incremental) + C8 (measures it) |
| AC-4 | Intraday pricing cadence | platform consumer — outcome, not a component |
| AC-5 / AC-6 | Incident load / maintenance down | operational outcome of the split |

---

## Build-status pointer (the sketches are stale here)

The sketches were written **before** the layers existed. Their inline status tags
(`built` / `missing` / `to build`) reflect authoring time only. **For live
status, do not trust the sketch words — use these sources:**

| Authority | Says what |
| --- | --- |
| **ADR-0001** (`docs/adrs/0001-analytical-backbone.md`) | Component A locked decisions; backbone described as built, green end-to-end |
| **ADR-0002** (`docs/adrs/0002-sentinel-engine.md`) | Component B locked decisions; crew described as built (79 passed / 6 skipped offline) |
| `platform/README.md` | Backbone layout + how to run C2–C5, C4h, C8 |
| `sentinel/README.md` | Crew layout (A1–A5, B1, B2, I4 scoring) + how to run |
| `CLAUDE.md` | Project handbook; current status framing |
| Agent memory: `backbone-locked-decisions.md` | U1/U2/U3 resolutions |

What changed since authoring: components the sketches call **missing / to build**
— C4h, C8, C5, and **all of Component B** (A1–A5, B1, B2) — **now exist** under
`platform/` and `sentinel/` with their own READMEs and tests. The sketch text
still calls several of them missing; that text is the staleness, not the code.

---

## How to read these, in order

1. **`analytical-backbone.md`** — Component A. Components C1–C5 (+ C4h, C8),
   AC map, build order (Phases 0–2), the interface exposed to B, open decisions.
2. **`sentinel-engine.md`** — Component B. The crew (A1–A5), the failure→
   capability map (all 14 generator failures → CrewAI features), the remediation
   loop, the interface consumed from A (I1–I5), build order (Phase 3).
3. **The ADRs** — for *what was actually decided* once the open `U#` items closed.

---

## Conventions these sketches obey

- **Plan altitude only** — no atomic tasks, no code. Both files state this up top.
- **One-way seam (R3)** — the I1–I5 table is the entire surface between A and B;
  if it's stable, the two build as independent tracks.
- **All-14-failures rule (R6)** — the crew must handle every failure the generator
  injects, not the 4 the tech spec names. The failure→capability map in
  `sentinel-engine.md` is the binding driver.
- **Verify by kind (R5)** — A by assertion (tied to AC-1…AC-6); B by scoring
  against I4 ground truth.
