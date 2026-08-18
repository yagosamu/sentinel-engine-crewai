"""AC-3 verdict and report assembly for the C8 freshness probe.

Takes a :class:`ProbeRun` (one or more inject -> ingest -> dbt -> gold samples)
and renders:

* a structured :class:`ProbeReport` (dict-serialisable, for evals/CI), and
* a human-readable text report.

The AC-3 PASS condition is precise and measured (not asserted):

    median(end_to_end_lag_s over samples) <= AC3_BUDGET_S   (default 300s / 5min)

The MEDIAN is the gate (not the max): AC-3 is a freshness SLO, and the median
is the statistic the brief commits to. The max is still reported so a tail
breach is visible to a reader.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass, field
from platform.probe.freshness import AC3_BUDGET_S, ProbeRun, SampleResult


@dataclass(slots=True)
class ProbeReport:
    """The structured AC-3 result for one probe run (dict-/JSON-serialisable).

    Carries the verdict, the median source->gold lag (the AC-3 statistic) plus
    min/max, the per-stage median breakdown, and every sample's detail.
    """

    passed: bool
    verdict: str
    budget_s: float
    samples: int
    # The AC-3 statistic and its companions, all in seconds.
    median_lag_s: float
    min_lag_s: float
    max_lag_s: float
    # Stage breakdown of the median sample (where the time goes).
    median_ingest_s: float
    median_dbt_s: float
    median_gold_wait_s: float
    # Every sample, for the audit trail.
    sample_details: list[dict] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


def build_report(run: ProbeRun, *, budget_s: float = AC3_BUDGET_S) -> ProbeReport:
    """Assemble the AC-3 report from a completed probe run."""
    samples: list[SampleResult] = run.samples
    if not samples:
        # No measurements taken — not a pass, and not a valid gate.
        return ProbeReport(
            passed=False,
            verdict="AC-3 INCONCLUSIVE — no samples were measured.",
            budget_s=budget_s,
            samples=0,
            median_lag_s=0.0,
            min_lag_s=0.0,
            max_lag_s=0.0,
            median_ingest_s=0.0,
            median_dbt_s=0.0,
            median_gold_wait_s=0.0,
            sample_details=[],
        )

    lags = [s.end_to_end_lag_s for s in samples]
    median_lag = statistics.median(lags)
    min_lag = min(lags)
    max_lag = max(lags)
    median_ingest = statistics.median(s.ingest_s for s in samples)
    median_dbt = statistics.median(s.dbt_s for s in samples)
    median_gold_wait = statistics.median(s.gold_wait_s for s in samples)

    passed = median_lag <= budget_s
    budget_min = budget_s / 60.0
    if passed:
        verdict = (
            f"AC-3 PASS — median source->gold lag {median_lag:.1f}s "
            f"(<= {budget_s:.0f}s / {budget_min:.0f}min budget) "
            f"across {len(samples)} sample(s); "
            f"min {min_lag:.1f}s / max {max_lag:.1f}s."
        )
    else:
        verdict = (
            f"AC-3 FAIL — median source->gold lag {median_lag:.1f}s "
            f"exceeds the {budget_s:.0f}s / {budget_min:.0f}min budget "
            f"across {len(samples)} sample(s); "
            f"min {min_lag:.1f}s / max {max_lag:.1f}s."
        )

    return ProbeReport(
        passed=passed,
        verdict=verdict,
        budget_s=budget_s,
        samples=len(samples),
        median_lag_s=round(median_lag, 3),
        min_lag_s=round(min_lag, 3),
        max_lag_s=round(max_lag, 3),
        median_ingest_s=round(median_ingest, 3),
        median_dbt_s=round(median_dbt, 3),
        median_gold_wait_s=round(median_gold_wait, 3),
        sample_details=[s.to_dict() for s in samples],
    )


def render_text(report: ProbeReport) -> str:
    status = "PASS" if report.passed else "FAIL"
    budget_min = report.budget_s / 60.0
    lines = [
        "=" * 72,
        f"  C8 FRESHNESS PROBE — AC-3 GATE   [{status}]",
        "=" * 72,
        "",
        f"  Budget (median):        {report.budget_s:.0f}s  ({budget_min:.0f} min)",
        f"  Samples:                {report.samples}",
        "",
        "  End-to-end lag (source COMMIT -> queryable in gold.gold_orders_obt):",
        f"    median:               {report.median_lag_s:.1f}s   <-- AC-3 statistic",
        f"    min / max:            {report.min_lag_s:.1f}s / {report.max_lag_s:.1f}s",
        "",
        "  Stage breakdown (median sample):",
        f"    C2 ingest:            {report.median_ingest_s:.1f}s",
        f"    dbt build:            {report.median_dbt_s:.1f}s",
        f"    gold poll wait:       {report.median_gold_wait_s:.1f}s",
    ]
    if report.sample_details:
        lines.append("")
        lines.append("  Per-sample:")
        for i, s in enumerate(report.sample_details, start=1):
            vis = "ok" if s["visible"] else "MISSING"
            lines.append(
                f"    [{i}] order_id={s['order_id']}  lag={s['end_to_end_lag_s']}s  "
                f"(ingest={s['ingest_s']}s dbt={s['dbt_s']}s gold={s['gold_wait_s']}s) [{vis}]"
            )
    lines.append("")
    lines.append(f"  VERDICT: {report.verdict}")
    lines.append("=" * 72)
    return "\n".join(lines)
