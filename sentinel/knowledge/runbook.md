# Sentinel Incident Disambiguation Runbook

This runbook is the **Knowledge source** (CrewAI `knowledge_sources`) the crew
consults when a single observation has more than one plausible root cause. It is
the `ambiguous_anomaly` unlock: the warehouse signal alone cannot decide between
two legitimate explanations, so the agent must reason against documented
operating doctrine instead of guessing.

Every entry maps a *symptom* to the *competing causes* and the *decisive
evidence* that separates them. Cite the deciding surface in the Diagnosis
`evidence_surface`.

---

## ambiguous_anomaly — revenue drop with two causes

**Symptom.** Daily revenue (`gold.gold_revenue_daily`) falls sharply, but no
rows land in any `silver_*_rejects` table — the data is *valid*, just smaller.

**Competing causes.**

1. **Cancellation surge.** A batch of recent orders flipped to
   `status = 'cancelled'`. Revenue falls because fewer orders are *realised*, not
   because anything is priced wrong.
2. **Price cut.** A set of products had `unit_price` halved. Revenue falls
   because each realised order is *worth less*, even though order volume is flat.

**Decisive evidence (how to separate them).**

- Query `silver.silver_orders` grouped by `status`: a spike in the `cancelled`
  count over the incident window points to **cancellation surge**.
- Query `silver.silver_products` for `unit_price` that dropped versus the prior
  baseline (a ~50% cut on a subset of products) points to **price cut**.
- If BOTH signals are present simultaneously, this is the canonical
  `ambiguous_anomaly`: report `failure_key = "ambiguous_anomaly"` and name BOTH
  contributing mechanisms in the summary. Do not collapse it to one cause when
  the evidence shows two — the whole point of this failure is that it is genuinely
  ambiguous, and an honest diagnosis says so.

**Diagnosis guidance.** `failure_key = "ambiguous_anomaly"`,
`evidence_surface = "silver_orders.status + silver_products.unit_price"`.

---

## recurring_incident — a repeat offender, not a new bug

**Symptom.** `silver.silver_orders_rejects` shows `reject_rule = 'negative_price'`
rows, and crew **memory** (or the `injected_incidents` history) shows the same
signature has been diagnosed before.

**Guidance.** The underlying defect is `negative_price`. The *value-add* of this
incident is recognising it is **recurring** — say "seen N times" rather than
cold-starting. Diagnose `failure_key = "negative_price"` (the substantive cause)
and note the recurrence in the summary / postmortem `recurrence_note`. The
scoring oracle credits `recurring_incident` and `negative_price` as aliases.

---

## destructive_fix — the fix is more dangerous than the defect

**Symptom.** Many rows have `total_amount = 0` (or otherwise corrupted) such that
the only remediation is a **bulk overwrite** of production data.

**Guidance.** This is the Human-in-the-loop case. A4 must NOT auto-propose and
apply a destructive bulk update. The propose-fix step PAUSES for human approval
(`human_input=True`). The proposed patch is written to `sentinel/proposed/` only
and is **never** applied to `platform/`. State explicitly in the postmortem that
the fix is destructive and requires sign-off.

---

## malformed_data — garbage in, typed report out

**Symptom.** `status` (or another text field) contains non-printable / injection
noise; rows land in `silver.silver_orders_rejects` with
`reject_rule = 'malformed_data'`.

**Guidance.** The source is unparseable, but the post-mortem must still be
**typed and validated** (`output_pydantic` + a guardrail). Summarise the garbage;
never echo it raw into a structured field. The guardrail rejects an empty or
untyped post-mortem and forces a retry.
