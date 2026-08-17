# CLAUDE.md — sentinel-engine-crewai

Project handbook for agents working in this repo. Read this first.

> **Numeric contract** (thresholds, agreement matrix): [`.claude/doctrine.yaml`](.claude/doctrine.yaml)
> **Operating rules** (enforceable conventions): [`.claude/rules/agent-operating-rules.md`](.claude/rules/agent-operating-rules.md)
> **The plans**: [`sketch/analytical-backbone.md`](sketch/analytical-backbone.md) · [`sketch/sentinel-engine.md`](sketch/sentinel-engine.md)

---

## What this project is

An **AI-native DataOps platform**. A high-volume e-commerce
company runs analytics and the storefront on one Postgres database; the two
workloads compete. The fix is a purpose-built analytical backbone that separates
them, monitored by an autonomous agent crew.

The system is **two components**:

- **Component A — Analytical backbone (Layers 1–5, deterministic).** Postgres
  source → Dagster ingestion → dbt Medallion (bronze/silver/gold) → DuckDB
  warehouse → FastAPI/MCP intelligence.
- **Component B — Sentinel engine (Layer 6, probabilistic).** A CrewAI
  hierarchical crew that watches A, diagnoses injected failures, and proposes
  fixes. Scored against ground truth, not asserted.

**Status today:** only **Layer 1 is built** — the Postgres schema, a deterministic
seeder, and a 14-mode chaos generator that logs ground truth to an
`injected_incidents` ledger (`src/db`, `src/seed`, `src/gen`). Layers 2–6 are
planned in `sketch/`. Everything else in this control layer exists so those
layers can be built correctly.

## What's actually in `src/`

| Package | Role |
|---------|------|
| `src/db` | Postgres schema (`01_schema.sql`) + connection/bulk helpers |
| `src/seed` | Deterministic clean baseline (frozen dataclasses, `Decimal` money) |
| `src/gen` | Chaos generator: 14 failures, traffic, `injected_incidents` ledger |

The `injected_incidents` table is the **architectural seam**: the generator writes
it (ground truth); the future Sentinel reads it as the scoring oracle.

## The agent fleet

**Tech specialists** — each tech has an *architect* (plans, no Bash) and a
*developer* (ships, has Bash). KBs in `.claude/kb/<tech>/`, grounded in official docs.

| Tech | Architect / Developer | Serves |
|------|----------------------|--------|
| `crewai` | `crewai-architect` / `crewai-developer` | Component B — the Sentinel crew |
| `dagster` | `dagster-architect` / `dagster-developer` | A · Layer 2 ingestion |
| `dbt` | `dbt-architect` / `dbt-developer` | A · Layer 3 transformation |
| `duckdb` | `duckdb-architect` / `duckdb-developer` | A · Layer 4 warehouse |
| `mcp` | `mcp-architect` / `mcp-developer` | A · Layer 5 intelligence |

**Universal closers** (run before merge, ground in the tech KBs at runtime):
`code-reviewer` · `code-simplifier` · `code-documenter`.

**Project agents** (pre-existing): `codebase-explorer`, `task-architect`, `caw-architect`.

### When to use which

1. **Designing before building?** → the `<tech>-architect`. Trade-offs, boundaries, ADRs.
2. **Writing code/tests/fixes?** → the `<tech>-developer`.
3. **Before merge?** → the closers, in order: reviewer → simplifier → documenter.
4. **Exploring an unfamiliar area?** → `codebase-explorer`.
5. **Turning intent into tasks?** → `task-architect`.

## Conventions every task follows

These are summarized from [`.claude/rules/agent-operating-rules.md`](.claude/rules/agent-operating-rules.md) — read it for the full text.

- **R1 Role boundary** — architects plan (no Bash), developers ship (Bash). Don't blur them.
- **R2 Ground every claim** — read the tech KB, then MCP docs, before asserting any API. Cite the source.
- **R3 One-way dependency** — B reads A read-only via the interface (I1–I5). A never depends on B.
- **R4 Respect U1–U3** — don't silently resolve the open scope/boundary/detection-seam decisions.
- **R5 Verify by kind** — A by assertions (tie to AC-1…AC-6); B by scoring against `injected_incidents`.
- **R6 Failure→capability map is binding** — the crew handles all 14 failures; build feature-by-feature.
- **R7 Chaos is reversible** — every inject→detect→score run restores a clean baseline first.
- **R8 Secrets & SQL** — no hardcoded secrets; parameterized SQL only.
- **R9 Generated files** — `AGENTS.md` / Cursor / Copilot are emitted; edit `.claude/` source and re-emit.

## The open decisions that gate work (U1–U3)

Do not assume these are settled:

- **U1 · Scope** — the engineering brief scopes Component B *and* the MCP layer
  **out of the committed program** ("evaluate after the foundation is proven").
  Confirm scope before building B or C5.
- **U2 · Raw boundary** — Dagster vs dbt-bronze owns the raw landing. Undecided.
- **U3 · Detection seam** — failures injected into raw Postgres; the Profiler reads
  gold DuckDB. Whether a defect survives the medallion is undefined. Name your
  assumption or you have no verifiable target.

## Acceptance criteria (Component A)

The brief's six pass/fail gates. **AC-1 is the early go/no-go.**

| AC | Proves | Covered by |
|----|--------|-----------|
| AC-1 | Peak isolation (75k orders/day, zero analytics lock-waits) | peak-load harness (C4h — to build) |
| AC-2 | Query latency p95 ≤ 5s | DuckDB gold OBTs |
| AC-3 | Freshness ≤ 5 min | Dagster incremental + freshness probe (C8 — to build) |
| AC-4 | Intraday pricing cadence | platform consumer |
| AC-5 / AC-6 | Incidents & maintenance down | operational outcome |

## Working in this repo

- **Run the source:** `make up` (Postgres), `make seed`, `make inject FAILURE=<key>`, `make watch`. See [README.md](README.md).
- **Package mgmt:** `uv`. **Lint:** `ruff` (`make lint`). No test suite yet (`src/` is untested — adding one is a known gap).
- **Tech KBs** carry `<!-- TODO -->` blocks — populate them from the cited docs as each layer is built; KB content quality determines agent and closer sharpness.
