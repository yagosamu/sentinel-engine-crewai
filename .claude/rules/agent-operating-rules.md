# Agent Operating Rules

Conventions every agent and task in this repo follows. These are enforceable
rules, not suggestions. Numeric values (thresholds, agreement matrix) live in
[`.claude/doctrine.yaml`](../doctrine.yaml) — this file is the prose contract.

---

## R1 · Role boundary (architect vs developer)

- **Architects plan. They do NOT have Bash and do NOT write production code.**
  Their output is decisions, trade-off matrices, ADRs. Code blocks in an
  architect's *output* describe shape, not shipped artifacts.
- **Developers ship. They have Bash and write code + tests.** Their output is
  working artifacts, not high-level decisions.
- If a task needs both, run the architect first, then hand its plan to the developer.
- The split is real because this stack has two verification models (see R5).

## R2 · Ground every claim

- Before asserting an API, flag, or behavior for a tech, **read its KB**
  (`.claude/kb/<tech>/`) first. If the KB is silent, consult MCP docs
  (`mcp__context7__*` / `mcp__ref__*`) — never answer a library question from memory.
- Apply the **Agreement Matrix** in `doctrine.yaml`: KB + MCP agree → high
  confidence; they conflict → investigate, don't pick. Cite the specific KB doc
  or doc URL per claim.
- KB docs themselves cite their source-of-truth URL. Keep that discipline when
  populating the `<!-- TODO -->` blocks.

## R3 · The two-component split is the law of the repo

- **Component A (analytical backbone, Layers 1–5)** is deterministic. See
  [`sketch/analytical-backbone.md`](../../sketch/analytical-backbone.md).
- **Component B (Sentinel engine, Layer 6)** is probabilistic. See
  [`sketch/sentinel-engine.md`](../../sketch/sentinel-engine.md).
- The dependency is **one-directional**: B reads A's exhaust read-only via the
  interface contract (I1–I5). A never depends on B. Do not introduce a coupling
  that makes A import, call, or wait on B.

## R4 · Respect the open decisions (U1–U3) — do not silently resolve them

These are unresolved and gate real work. A task must not assume an answer:

- **U1 · Scope.** The brief scopes Component B (and the MCP layer C5) *out* —
  "evaluate after the foundation is proven." Do not build B or C5 as if approved.
  If a task touches them, confirm scope first.
- **U2 · Raw boundary.** Who owns the raw landing — Dagster (C2) or dbt bronze
  (C3)? Undecided. A task crossing this seam must name its assumption.
- **U3 · Detection seam.** Failures are injected into raw Postgres; the Profiler
  (A3) reads gold DuckDB. Whether a defect survives bronze→silver→gold is
  **undefined**. Any A3 detection task must state which layer it expects the
  defect to be visible in, or it has no verifiable target.

## R5 · Verify by kind

- **Component A work** is verified by assertions: did the row land, did the
  transform produce the right number, does the gold query hit the latency budget?
  Tie A tasks to the acceptance criteria they serve (AC-1…AC-6).
- **Component B work** is verified by scoring against ground truth: did the crew's
  diagnosis match the `injected_incidents` row? Never assert B's output is
  "correct" — score it against the oracle (I4).
- **AC-1 is the go/no-go gate.** Isolation-under-peak work is verified by a
  75k-orders/day load test with lock-wait audit, not by latency numbers alone.

## R6 · The CrewAI failure→capability map is binding

The crew must handle all **14** generator failures, not the 4 the tech spec
names. Each advanced failure forces a specific CrewAI feature — the mapping is in
[`.claude/kb/crewai/reference/capability-unlock-map.md`](../kb/crewai/reference/capability-unlock-map.md)
and `src/gen/failures.py` `unlocks` fields. Build the crew feature-by-feature
against it; do not scope an agent that can't handle a failure already in the registry.

## R7 · Chaos is reversible or it isn't trustworthy

Failure injectors drop constraints with no restore path; `reset-schema` only
reverts the column rename. **Every "inject → detect → score" run must restore a
clean baseline first**, or its result is non-reproducible. Treat reset-to-clean
as part of the eval, not an afterthought.

## R8 · Secrets and SQL

- No hardcoded secrets or credentials in any file. `.env` is gitignored; keep it so.
- All SQL parameters use placeholders (`%s` / bound params). Identifiers may be
  interpolated only from internal constants or `information_schema`, never from
  user input.

## R9 · Generated files are not hand-edited

`AGENTS.md`, `.cursor/rules/*.mdc`, `.github/copilot-instructions.md` are emitted
from `.claude/` by `emit-cross-tool.sh`. Edit the `.claude/` source (agents,
`doctrine.yaml`, `_index.yaml`) and re-emit. Hand-edits get a `.proposed` sibling,
not a clobber — reconcile them back into source.
