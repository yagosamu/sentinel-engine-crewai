"""AC-1 verdict and report assembly for the C4h harness.

Takes the raw monitor + load results and renders:

* a structured :class:`HarnessReport` (dict-serialisable, for evals/CI), and
* a human-readable text report.

The AC-1 PASS condition is precise and measured (not asserted):

    count(analytics-attributable lock-waits) <= max_analytics_lock_waits
    (default 0)

i.e. no transactional-path session (``oltp_writer``) was ever observed blocked
on a lock held by an analytics session (``dagster_ingest``) across the run.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from platform.harness.config import HarnessConfig
from platform.harness.loadgen import LatencySummary
from platform.harness.monitor import LockWaitEvent, MonitorResult


@dataclass(slots=True)
class HarnessReport:
    """The structured AC-1 result for one harness run (dict-/JSON-serialisable).

    Carries the verdict, whether the load floor was met, per-role latency
    summaries, and the lock-wait attribution counts an eval/CI reads.
    """

    profile: str
    passed: bool
    verdict: str
    # Whether the run applied enough load to be a valid peak-load test.
    load_floor_met: bool
    # Workload realised.
    duration_s: float
    min_orders_per_day_floor: int
    achieved_orders_per_day: float
    orders_committed: int
    # Latency summaries per role.
    oltp: dict
    analytics: dict
    # AC-1 attribution.
    samples_taken: int
    sample_errors: int
    analytics_attributable_lock_waits: int
    other_lock_waits: int
    max_allowed_analytics_lock_waits: int
    # A few example offending edges, for debugging a FAIL.
    examples: list[dict] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


def _event_to_dict(e: LockWaitEvent) -> dict:
    return {
        "waiter_pid": e.waiter_pid,
        "waiter_app": e.waiter_app,
        "blocker_pid": e.blocker_pid,
        "blocker_app": e.blocker_app,
        "wait_event_type": e.wait_event_type,
        "wait_event": e.wait_event,
        "waiter_query": e.waiter_query,
    }


def build_report(
    config: HarnessConfig,
    duration_s: float,
    oltp: LatencySummary,
    analytics: LatencySummary,
    monitor: MonitorResult,
) -> HarnessReport:
    attributable = monitor.analytics_attributable
    n_attributable = len(attributable)
    no_lock_waits = n_attributable <= config.max_analytics_lock_waits

    achieved_per_day = (oltp.rows_committed / duration_s) * 86_400 if duration_s > 0 else 0.0
    load_floor_met = achieved_per_day >= config.min_orders_per_day

    # AC-1 PASS requires BOTH a clean transactional path AND that we actually
    # applied a peak-scale load (otherwise a clean result proves nothing).
    passed = no_lock_waits and load_floor_met

    if not load_floor_met:
        verdict = (
            f"AC-1 INCONCLUSIVE — achieved only {achieved_per_day:,.0f} orders/day-equiv, "
            f"below the {config.min_orders_per_day:,} floor; the run did not apply peak load, "
            f"so the {'clean' if no_lock_waits else 'observed-contention'} result is not a valid gate. "
            f"(analytics-attributable lock-waits seen: {n_attributable})"
        )
    elif no_lock_waits:
        verdict = (
            f"AC-1 PASS — {n_attributable} analytics-attributable lock-wait(s) "
            f"on the transactional path (threshold {config.max_analytics_lock_waits}) "
            f"under {achieved_per_day:,.0f} orders/day-equiv peak load; the analytics load "
            f"('dagster_ingest') did not block the OLTP path ('oltp_writer')."
        )
    else:
        verdict = (
            f"AC-1 FAIL — observed {n_attributable} lock-wait(s) where an OLTP writer "
            f"was blocked by an analytics session (threshold {config.max_analytics_lock_waits}) "
            f"under {achieved_per_day:,.0f} orders/day-equiv peak load."
        )

    return HarnessReport(
        profile=config.profile,
        passed=passed,
        verdict=verdict,
        load_floor_met=load_floor_met,
        duration_s=round(duration_s, 3),
        min_orders_per_day_floor=config.min_orders_per_day,
        achieved_orders_per_day=round(achieved_per_day, 1),
        orders_committed=oltp.rows_committed,
        oltp=asdict(oltp),
        analytics=asdict(analytics),
        samples_taken=monitor.samples_taken,
        sample_errors=monitor.sample_errors,
        analytics_attributable_lock_waits=n_attributable,
        other_lock_waits=len(monitor.other_lock_waits),
        max_allowed_analytics_lock_waits=config.max_analytics_lock_waits,
        examples=[_event_to_dict(e) for e in attributable[:5]],
    )


def render_text(report: HarnessReport) -> str:
    status = "PASS" if report.passed else "FAIL"
    lines = [
        "=" * 72,
        f"  C4h PEAK-LOAD HARNESS — AC-1 GATE   [{status}]   profile={report.profile}",
        "=" * 72,
        "",
        f"  Window:                 {report.duration_s:.2f}s",
        f"  Load floor (validity):  {report.min_orders_per_day_floor:,} orders/day-equiv  "
        f"[{'met' if report.load_floor_met else 'NOT MET'}]",
        f"  Achieved load:          {report.achieved_orders_per_day:,.0f} orders/day-equiv "
        f"({report.orders_committed:,} orders committed)",
        "",
        "  Transactional path (application_name='oltp_writer'):",
        f"    commits:              {report.oltp['count']:,}  "
        f"(errors {report.oltp['errors']}, timeouts {report.oltp['timeouts']})",
        f"    commit latency ms:    p50={report.oltp['p50_ms']}  p95={report.oltp['p95_ms']}  "
        f"p99={report.oltp['p99_ms']}  max={report.oltp['max_ms']}",
        "",
        "  Analytics path (application_name='dagster_ingest'):",
        f"    queries:              {report.analytics['count']:,}  "
        f"(errors {report.analytics['errors']}, timeouts {report.analytics['timeouts']})",
        f"    query latency ms:     p50={report.analytics['p50_ms']}  p95={report.analytics['p95_ms']}  "
        f"p99={report.analytics['p99_ms']}  max={report.analytics['max_ms']}",
        "",
        "  Lock-wait attribution (pg_blocking_pids over the run):",
        f"    samples taken:        {report.samples_taken:,}  (sample errors {report.sample_errors})",
        f"    analytics-attributable lock-waits:  {report.analytics_attributable_lock_waits}  "
        f"(threshold {report.max_allowed_analytics_lock_waits})",
        f"    other lock-waits (not attributable):{report.other_lock_waits}",
        "",
        f"  VERDICT: {report.verdict}",
    ]
    if report.examples:
        lines.append("")
        lines.append("  Offending edges (first 5):")
        for ex in report.examples:
            lines.append(
                f"    pid {ex['waiter_pid']} [{ex['waiter_app']}] blocked by "
                f"pid {ex['blocker_pid']} [{ex['blocker_app']}] "
                f"on {ex['wait_event_type']}/{ex['wait_event']}"
            )
    lines.append("=" * 72)
    return "\n".join(lines)
