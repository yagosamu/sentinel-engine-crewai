# tests/ — the verification layer

The pytest tree that proves the platform works. It mirrors the project's
two-component split plus the AC-1 peak-load gate, in **three suites** with **two
verification models**: Component A is verified by *assertion* (did the row land,
did the number tie back, is the latency within budget); Component B is verified by
*scoring* against the I4 ground-truth oracle (did the crew's diagnosis match the
`injected_incidents` row). Every module is engineered to **stay green offline** and
**never fabricate a result** — the project honesty rule and R5.

> This README is the contributor handbook for `tests/`. Project-wide context (the
> two-component split, the agent fleet, the operating rules, the open decisions)
> lives in [`../CLAUDE.md`](../CLAUDE.md) and
> [`../.claude/rules/agent-operating-rules.md`](../.claude/rules/agent-operating-rules.md);
> the environment contract and single-writer rule are shared with
> [`../platform/README.md`](../platform/README.md). They are referenced here, not
> duplicated. The locked decisions the e2e tests assert against are recorded in
> [`../docs/adrs/0001-analytical-backbone.md`](../docs/adrs/0001-analytical-backbone.md)
> and [`../docs/adrs/0002-sentinel-engine.md`](../docs/adrs/0002-sentinel-engine.md).

---

## WHAT — three suites

| Suite | Dir | Verifies | Model |
|-------|-----|----------|-------|
| Component A — analytical backbone | root `tests/*.py` | C2 ingest, C5 intelligence, C8 freshness, full + incremental e2e | assertion (R5) |
| Component B — Sentinel crew | [`sentinel/`](sentinel) | tools, scoring rubric, crew assembly, flow, RAG, trigger, e2e diagnose, live eval | scoring vs I4 oracle (R5) |
| C4h peak-load harness | [`harness/`](harness) | AC-1 isolation: lock-wait attribution under peak load | assertion |

[`conftest.py`](../conftest.py) at the **repo root** is load-bearing for every
Component-A and harness test: it evicts the stdlib `platform` module from
`sys.modules` so the repo's top-level `platform` *package* wins. Without it every
`from platform.* import ...` is `ModuleNotFoundError`.

---

## WHY — the design invariants the suite enforces

- **Offline is green, never faked.** Each module that needs live infrastructure
  *skips* itself rather than failing or pretending. The gating is uniform: DB tests
  skip on unreachable Postgres, C5 skips when gold is not materialized, Sentinel
  modules `importorskip` their package, the live-LLM eval skips without an API key.
  A clean checkout with no stack running is green.
- **Plumbing is proved separately from intelligence.** Component B has an offline
  ladder (below). The `StubLLM` seam verifies *wiring* only and never claims the
  diagnosis is right; only `test_eval_live` exercises a real LLM, and even then it
  **reports** the score rather than asserting it is "correct".
- **Single-writer is respected throughout.** Warehouse-touching tests serialize;
  the e2e subprocess helpers retry only on transient DuckDB lock conflicts; raw
  truncation runs as a separate fully-exiting process so the writer lock releases
  first. See the hazard note under HOW.
- **Chaos is reversible (R7).** Every inject run restores a clean baseline (reseed
  + clear the I4 ledger + flush raw + rebuild) in a `finally` block or autouse
  fixture, so an interrupted run never poisons the next one.
- **The dependency stays one-way (R3).** Tests assert against `platform/*`,
  `sentinel/*`, and `src/gen/*`. Component B reads Component A's exhaust read-only;
  no test introduces a coupling that makes A depend on B.

---

## HOW — running the suite

All commands are from the repo root. There is **no `make test` target** and no
`pytest` addopts beyond `pythonpath = ["."]` — run pytest directly with the
environment contract set (the same three vars `platform/` needs):

```bash
export PYTHONPATH=$(pwd)          # the local `platform/` package shadows stdlib platform
export DUCKDB_DATABASE=$(pwd)/platform/warehouse/warehouse.duckdb
export DAGSTER_HOME=$(pwd)/.dagster_home

uv run pytest tests/             # full default offline run (gated modules self-skip)
uv run pytest tests/sentinel/    # one suite
uv run pytest tests/test_c8_probe.py -q
```

> **DuckDB is single-writer.** Warehouse-touching tests (C2, C5, both e2e modules)
> serialize by design — never run two warehouse readers/writers at once, and do
> **not** launch `dagster dev` alongside them. If a run reports "database is
> locked", a stale holder is open; clear it before retrying:
> ```bash
> lsof -t platform/warehouse/warehouse.duckdb | xargs -r kill -9
> ```
> This hazard is otherwise encoded only in the suite's retry loops, not stated in
> code.

### The Component B offline ladder

Component B is layered so you can prove more with more dependencies. Know which
rung you are on:

| Rung | What it proves | Modules | Needs |
|------|----------------|---------|-------|
| L1 — pure / mocked | logic in isolation | `test_rubric`, `test_scoring`, `test_rag`, `test_flow` | nothing |
| L2 — StubLLM wiring | the **plumbing** (typed coercion, guardrails, HITL, routing) — *not* diagnostic correctness | `test_crew_build`, `test_crew_stub`, `test_tools`, `test_trigger` | nothing |
| L2 — live pipeline | inject -> detect -> score loop against the live oracle, with a stub key emitting the correct answer | `test_e2e_diagnose` | Postgres |
| L3 — live LLM | a real crew run, **reported** not asserted correct | `test_eval_live` | `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` |

[`sentinel/stub_llm.py`](sentinel/stub_llm.py) is the offline seam: a
`StubLLM(crewai.LLM)` that returns scripted text (it must subclass `LLM` because
crewai 0.100.0 coerces non-`LLM` objects into token-calling LLMs).

---

## WHERE — the run matrix

One row per module. The module-level docstrings carry the detailed contract; this
table is the directory-level index of **how to enable each one**. The default
offline run (`uv run pytest tests/`) executes every "no gate" row and skips the
rest with an explicit reason.

### Component A — backbone (assertion-verified)

| Module | Covers | Gate to enable |
|--------|--------|----------------|
| [`test_c2_ingestion.py`](test_c2_ingestion.py) | C2 raw ingest invariants: row-count mirror, `_ingested_at` stamp (AC-3 anchor), DECIMAL/tz fidelity, idempotent re-materialization | Postgres reachable (else skip) |
| [`test_c5_intelligence.py`](test_c5_intelligence.py) | C5 tools return real gold rows that tie back to direct DuckDB roll-ups; SQL read-only guard; FastAPI TestClient parity; live MCP JSON-RPC handshake; schema reconcile | gold materialized (else skip) |
| [`test_c8_probe.py`](test_c8_probe.py) | AC-3 freshness: pure verdict math (median is the gate, exactly-at-budget passes, no-samples is INCONCLUSIVE) **always runs**; one live beacon | live beacon needs Postgres (pure math: no gate) |
| [`test_e2e_backbone.py`](test_e2e_backbone.py) | Full L1->L5 on the live DB, full-refresh: gold grain + U3 defect-survival (defect in the right `silver_*_rejects`, absent from gold) + C5 tie-back | `RUN_E2E_BACKBONE` in {1,true,yes,on} **and** Postgres **and** initialized warehouse |
| [`test_e2e_incremental_medallion.py`](test_e2e_incremental_medallion.py) | Round-1 BLOCKER regression on the **incremental** path: `late_arrival` flagged into gold, `destructive_fix` re-extracted and stale clean gold replaced (inject order matters) | `RUN_E2E_BACKBONE` + Postgres + persistent `DAGSTER_HOME` |

### Component B — Sentinel (scoring-verified, `tests/sentinel/`)

| Module | Covers | Gate to enable |
|--------|--------|----------------|
| [`stub_llm.py`](sentinel/stub_llm.py) | shared `StubLLM(crewai.LLM)` — the offline no-key seam (helper, not a test) | n/a |
| [`test_scoring.py`](sentinel/test_scoring.py) | legacy `score_diagnosis(key)->str` shim over a mocked I4 connection | `importorskip` sentinel |
| [`test_rubric.py`](sentinel/test_rubric.py) | graded `score_run` tiers (exact / alias / cascade partial-credit / miss / no-run) + non-gating evidence dimension | `importorskip` sentinel |
| [`test_tools.py`](sentinel/test_tools.py) | read-only tools vs temp fixtures: I1 Dagster logs, I2 dbt run_results, I3 ProfileRejects + QueryDuckDB (allow-list enforced) | `importorskip` sentinel |
| [`test_crew_build.py`](sentinel/test_crew_build.py) | crew assembly (no kickoff): hierarchical process, manager off the roster, wired tools, typed tasks, HITL off-by-default, memory plumbing | `importorskip` sentinel |
| [`test_crew_stub.py`](sentinel/test_crew_stub.py) | real task machinery via StubLLM: pydantic coercion, guardrail reject-then-retry, destructive HITL gating (sequential single tasks) | `importorskip` sentinel |
| [`test_flow.py`](sentinel/test_flow.py) | deterministic cascade Flow + a crewai-0.100.0 `@router` list-return regression guard | `importorskip` sentinel |
| [`test_rag.py`](sentinel/test_rag.py) | I5 incident RAG: token-overlap ranking + recurrence count over a mocked ledger; cold-start-empty | `importorskip` sentinel |
| [`test_trigger.py`](sentinel/test_trigger.py) | B1 trigger poll/dispatch/route over a fixture ledger: cursor advance, base vs cascade, destructive=human_gated, crew-crash => honest no-run | `importorskip` sentinel |
| [`test_e2e_diagnose.py`](sentinel/test_e2e_diagnose.py) | L2 live-pipeline capstone: real inject + score vs live oracle with a stub key; **coverage meta-test asserts all 14 failures (R6)** | Postgres (else skip) |
| [`test_eval_live.py`](sentinel/test_eval_live.py) | L3 the only live-LLM module: runs the real crew loop, **reports** the ScoreResult + reproducible re-grade, never asserts "correct" | `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` |

### C4h harness (AC-1, `tests/harness/`)

| Module | Covers | Gate to enable |
|--------|--------|----------------|
| [`test_verdict.py`](harness/test_verdict.py) | pure AC-1 decision rule: clean+load=PASS, analytics-attributable wait=FAIL, below-floor=INCONCLUSIVE, writer-vs-writer does **not** fail AC-1 | none |
| [`test_lockwait_detection.py`](harness/test_lockwait_detection.py) | live true-positive: a tagged `dagster_ingest` session blocking an `oltp_writer` MUST be flagged analytics-attributable; clean smoke PASS; CLI exit-0 | Postgres reachable/seeded (else skip) |

### Enabling the gated rows

```bash
# Bring up + seed the source, then build a warehouse (needed for C5 / e2e):
make up && make seed
make ingest-once && make dbt-build        # materializes gold; enables test_c5_intelligence

# The destructive live e2e (mutates the live DB + shared warehouse, restores a
# baseline after). Opt-in flag is the safety latch:
RUN_E2E_BACKBONE=1 uv run pytest tests/test_e2e_backbone.py
RUN_E2E_BACKBONE=1 uv run pytest tests/test_e2e_incremental_medallion.py

# The live-LLM scorecard (L3) — the only module that calls a real model:
OPENAI_API_KEY=sk-... uv run pytest tests/sentinel/test_eval_live.py -s
```

---

## Conventions specific to this tree

- **The 14-failure coverage tripwire.** The coverage meta-test in
  `test_e2e_diagnose.py` binds the suite to the full
  `src.gen.failures.REGISTRY` (R6). Adding a new generator failure **will fail
  that test** until the e2e parameter list is extended — this is the intended
  tripwire, not a flake. Update the param list when you add a failure mode.
- **Skip guards are per-module by design.** Several modules re-implement a private
  `_postgres_reachable` / `_postgres_ready` guard rather than sharing one, so a
  single module can be run in isolation without importing the rest of the suite.
- **Expensive builds are shared, not repeated.** Module-scoped fixtures
  (`loaded_warehouse`, `backbone_run`, `incremental_run`) drive one build and many
  assertions read it. The `_temp_instance_ref` Dagster persistent-instance helper
  is what makes the incremental tests genuinely incremental.
- **Module docstrings are the detailed spec.** `test_e2e_backbone`,
  `test_e2e_diagnose`, `test_e2e_incremental_medallion`, and `stub_llm` carry
  exemplary docstrings — read those for the per-module contract; this README is the
  index, not a substitute.

### Known gap (for the next closer pass)

`test_c5_intelligence.py` uses `@pytest.mark.anyio` but no marker is registered in
`pyproject.toml`, so pytest emits an unregistered-marker warning (and would break
under `--strict-markers`). Registering `anyio` — and adding `live` / `e2e` markers
so the gated modules can be selected by marker instead of env var only — is a clean
follow-up. There is currently no way to select e2e vs unit by marker.
