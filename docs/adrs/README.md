# docs/adrs/ — Architecture Decision Records

A sequential, append-only log of the decisions that gate this project. Each ADR
records one set of load-bearing choices, the context that forced them, and the
consequences — so a decision that will be re-litigated in six months is answered
by a file, not by memory.

> The two-component split, the open-decision framing (U1–U3), and the operating
> rules these ADRs reference are defined in [`../../CLAUDE.md`](../../CLAUDE.md)
> and [`../../.claude/rules/agent-operating-rules.md`](../../.claude/rules/agent-operating-rules.md).
> The reference PDFs the ADRs build on are indexed in [`../README.md`](../README.md).

---

## The log

| ADR | Title | Status | Date | Component | Locks |
|-----|-------|--------|------|-----------|-------|
| [0001](0001-analytical-backbone.md) | Analytical backbone | Accepted | 2026-06-29 | A (`../../platform/`) | U2 (Dagster owns raw), U3 (quarantine, don't drop), CDC scheme, bulk-upsert fix, dbt-in-Dagster one-DAG |
| [0002](0002-sentinel-engine.md) | Sentinel engine | Accepted | 2026-06-30 | B (`../../sentinel/`) | hierarchical crew + custom manager, two-tier scoring rubric, Flow only for the cascade, poll-the-ledger trigger, gated/HITL B→A proposals, stub-LLM + key-gated live verification |

Each ADR closes the relevant open decisions **for its component as scoped** and
cites the shipped code that depends on them.

---

## Naming convention

```
NNNN-short-slug.md
```

- **`NNNN`** — a zero-padded, four-digit sequence number, never reused. The next
  ADR is `0003`.
- **`short-slug`** — a few lowercase, hyphen-separated words naming the subject
  (the component or decision area), e.g. `analytical-backbone`, `sentinel-engine`.
- The log is **append-only**. To reverse a past decision, write a new ADR that
  supersedes it and set the old one's status to `Superseded by ADR-NNNN`; do not
  edit the accepted record in place.

---

## How to add ADR-0003+

1. **Decide it is worth an ADR.** Record a decision only when it is non-obvious
   **and** likely to be re-litigated ("we will be asked about this again").
   Trivial or self-evident choices do not earn an ADR.
2. **Copy the template below** to `NNNN-short-slug.md` with the next number.
3. **Honor the structure the existing ADRs use** — a scope blockquote up top
   (referencing, not restating, CLAUDE.md and the rules), numbered `Decision N`
   sections each with its own `**Why:**`, a `Consequences` section split into
   Positive / Negative-trade-offs / Neutral, and a `Citations` list.
4. **Cite ground truth.** Every decision cites the handbook/rules it rests on and
   the specific code (`path:symbol`) it governs. Citations point into the live
   tree (`../../platform/`, `../../sentinel/`, `../../src/`) and can drift —
   reference real, current paths.
5. **Add a row** to the log table above and to the ADR table in
   [`../README.md`](../README.md).

### Template

```markdown
# ADR-NNNN: <title>

**Status:** Proposed | Accepted | Superseded by ADR-NNNN
**Date:** YYYY-MM-DD

> Scope: what this ADR gates. Reference the repo-wide framing in
> ../../CLAUDE.md and ../../.claude/rules/agent-operating-rules.md — do not
> restate it.

## Context

Why this decision is needed now. The forces in tension; what is open.

---

## Decision 1 — <the choice>

<what was decided, in specifics>

**Why:** <the reasoning that makes this the right call>

---

## Decision 2 — <the choice>

...

---

## Consequences

**Positive:**
- ...

**Negative / trade-offs:**
- ...

**Neutral:**
- ...

## Citations

- Project handbook: ../../CLAUDE.md (<what it provides>)
- Operating rules: ../../.claude/rules/agent-operating-rules.md (<which rules>)
- Code — Decision N: `<path>` (`<symbol>`)
```

> The `Proposed → Accepted` lifecycle is per the project's existing ADRs (both
> 0001 and 0002 shipped as `Accepted`). An architect agent typically drafts an
> ADR; it is filed here once the decision is locked.
