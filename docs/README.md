# docs/ — references and decision records

The project's documentation store. No code lives here. It holds two kinds of
document: the upstream **"why / what"** as a set of reference PDFs (the business
case, the engineering problem statement, the canonical technical design, and the
"Converge" methodology), and the downstream **"what we decided"** as a sequential
[Architecture Decision Record](adrs/) log.

> This README indexes `docs/` only. Project-wide context — the two-component
> split, the agent fleet, the open decisions (U1–U3), the acceptance criteria —
> lives in [`../CLAUDE.md`](../CLAUDE.md) and
> [`../.claude/rules/agent-operating-rules.md`](../.claude/rules/agent-operating-rules.md);
> it is referenced here, not duplicated. The contributor handbook for the
> deterministic backbone is [`../platform/README.md`](../platform/README.md).

---

## WHAT — what is in this folder

### Reference PDFs (the inputs)

All five are binary PDFs at the root of `docs/`. They are inputs the project was
built from; they are opaque to `grep`/`diff`/`git` and to agents that must ground
claims (R2) — read them directly.

| File | Pages | Author / source | Date | Stated status | What it is |
|------|-------|-----------------|------|---------------|------------|
| [`engineering-brief.pdf`](engineering-brief.pdf) | 6 | Data Engineering | 2026-06-20 | For review and alignment | "Pass 1 of 2." Problem-only — names **no** technology. The vitals (12 min → 5 s query, 60–70% → 20–30% utilization, 24 hr → 5 min freshness) and the $7.6M / 3-yr cost-of-inaction. This is the document `CLAUDE.md` cites for the U1 scope-out. |
| [`business-requirements-document.pdf`](business-requirements-document.pdf) | 36 | Finance & Technology Leadership | 2026-06-18 | For Review and Approval | The BRD: a 12-section business case (~50k orders/day, 3-year financials) plus a target-architecture appendix. |
| [`tech-spec-analytics-backbone-sentinel-engine.pdf`](tech-spec-analytics-backbone-sentinel-engine.pdf) | 10 | Manus AI | 2026-06-18 | — | **The canonical architecture.** Names the stack (Dagster / dbt / DuckDB + FastAPI / MCP + CrewAI), carries the full 6-layer mermaid diagram (page 4), and casts the Synthetic Data Generator as a permanent validation harness. |
| [`converge-spine-methodology.pdf`](converge-spine-methodology.pdf) | 6 | — | — | — | The "Converge" meta-methodology in document form: **five universal passes + one fork** (Intent → Structure → Decomposition → Consensus → Harness FORK). How the work is produced, not project content. |
| [`cvg-aut-systems-spine.pdf`](cvg-aut-systems-spine.pdf) | 12 | — | — | — | The **slide-deck** form of the same Converge thesis (landscape, ~3.6 MB, the heaviest file). The slug matches [`../presentation/cvg-aut-systems-spine-deck.html`](../presentation/cvg-aut-systems-spine-deck.html). |

> **The two "Converge" files are not duplicates — and they disagree.** Both share
> the same title and thesis, but `converge-spine-methodology.pdf` describes
> **five** passes + one fork, while the `cvg-aut-systems-spine.pdf` deck describes
> **seven** passes. Treat the `-methodology` PDF as the prose reference and the
> `cvg-aut-systems-spine` PDF as its presentation companion. The five-vs-seven
> pass-count difference **may be stale** — it cannot be resolved from these
> documents alone. Flag it for the author before quoting a pass count.

### Decision records (the outputs)

[`adrs/`](adrs/) is a sequential ADR log — the "what we decided" half. It locks
the open questions (U1–U3) that the references left open and binds each decision
to the shipped code in [`../platform/`](../platform) and [`../sentinel/`](../sentinel).
See [`adrs/README.md`](adrs/README.md) for the log index, the naming convention,
and how to add the next ADR.

| ADR | Title | Status | Date | Component | In one line |
|-----|-------|--------|------|-----------|-------------|
| [0001](adrs/0001-analytical-backbone.md) | Analytical backbone | Accepted | 2026-06-29 | A (`platform/`) | Five locked decisions: Dagster-owns-raw, quarantine-not-drop, timestamp + 2-PK-arm CDC, set-based Arrow bulk-upsert, dbt wired into the Dagster DAG. |
| [0002](adrs/0002-sentinel-engine.md) | Sentinel engine | Accepted | 2026-06-30 | B (`sentinel/`) | Six locked decisions: hierarchical crew with a custom manager (A1), two-tier scoring rubric, a Flow only for `multi_failure_cascade`, the B1 trigger polls I4, gated/HITL proposals, stub-LLM offline + key-gated live scoring. |

---

## WHY — the documentary lineage

The documents form a chain from problem to decision. Read in this order:

```
engineering-brief.pdf      problem only — names no technology, sets the vitals
        │
        ▼
business-requirements-document.pdf   business case + target-architecture appendix
        │
        ▼
tech-spec-…-sentinel-engine.pdf      CANONICAL design — names the stack, 6-layer diagram
        │
        ▼
adrs/0001 + adrs/0002                 decisions that lock U1–U3 against shipped code
```

The two Converge PDFs sit **beside** this chain, not inside it: they are the
meta-methodology (how the work is produced via passes and eval gates), not
content about the e-commerce platform.

A note on review status: the brief ("For review and alignment") and the BRD
("For Review and Approval") are review-stage inputs, yet the **Accepted** ADRs
already build on them. That is expected for a workshop build — the ADRs record
what was implemented; the upstream PDFs were never formally signed off.

---

## HOW — reading and citing these documents

- **Reading the PDFs.** Open them in a PDF viewer. They have no markdown or text
  companion, so they cannot be searched, diffed, or grounded against by tooling.
  When you need to ground a claim per R2, cite the PDF **by name and page** (e.g.
  "tech-spec, page 4, the 6-layer diagram").
- **Citing an ADR.** Link the ADR file and the decision number (e.g.
  `adrs/0001-analytical-backbone.md`, Decision 2). ADRs carry their own
  source-of-truth citations at the bottom.
- **ADR citations point into the live tree.** Both ADRs cite specific files in
  `../platform/` and `../sentinel/` (outside this folder). Those paths can drift
  as code moves — verify a cited path resolves before relying on it; do not assume
  it still exists because the ADR names it.

---

## WHERE — adding to this folder

- **A new reference PDF** → drop it at the root of `docs/` and add a row to the
  reference table above (file, pages, author/date, stated status, one-line
  purpose).
- **A new decision** → add an ADR under [`adrs/`](adrs/). The naming convention,
  template, and numbering rule are in [`adrs/README.md`](adrs/README.md).
- **Do not** add a top-level repo handbook here — that is [`../CLAUDE.md`](../CLAUDE.md).
  Do not duplicate the `platform/` or `sentinel/` contributor guides — link them.
