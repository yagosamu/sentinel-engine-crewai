"""Unit tests for the C4h verdict logic — no database required.

These pin the AC-1 decision rule itself:
  * clean + enough load            -> PASS  (exit 0)
  * analytics-attributable wait    -> FAIL  (exit 1)
  * not enough load                -> INCONCLUSIVE (exit 3), even if clean
"""

from __future__ import annotations

from platform.harness.config import smoke_profile
from platform.harness.loadgen import LatencySummary, WorkerStats
from platform.harness.monitor import LockWaitEvent, MonitorResult
from platform.harness.verdict import build_report


def _summary(role: str, ops: int, rows: int) -> LatencySummary:
    stats = WorkerStats(role=role, operations=ops, rows_committed=rows, latencies_ms=[1.0, 2.0, 3.0])
    return LatencySummary.from_stats(stats)


def _attributable_event() -> LockWaitEvent:
    # waiter=oltp_writer, blocker=dagster_ingest -> attributable to analytics.
    return LockWaitEvent(
        sampled_at=0.0,
        waiter_pid=1,
        waiter_app="oltp_writer",
        blocker_pid=2,
        blocker_app="dagster_ingest",
        wait_event_type="Lock",
        wait_event="relation",
        waiter_query="INSERT ...",
    )


def _other_event() -> LockWaitEvent:
    # writer blocked by another writer -> NOT analytics-attributable.
    return LockWaitEvent(
        sampled_at=0.0,
        waiter_pid=1,
        waiter_app="oltp_writer",
        blocker_pid=3,
        blocker_app="oltp_writer",
        wait_event_type="Lock",
        wait_event="transactionid",
        waiter_query="INSERT ...",
    )


def test_pass_when_clean_and_load_met() -> None:
    config = smoke_profile()
    duration = 4.0
    # 20000 rows over 4s -> ~432M/day, well over the smoke floor (10k).
    oltp = _summary("oltp_writer", ops=2000, rows=20_000)
    analytics = _summary("dagster_ingest", ops=500, rows=0)
    report = build_report(config, duration, oltp, analytics, MonitorResult(samples_taken=40))
    assert report.passed is True
    assert report.load_floor_met is True
    assert report.analytics_attributable_lock_waits == 0
    assert "AC-1 PASS" in report.verdict


def test_fail_when_analytics_attributable_wait() -> None:
    config = smoke_profile()
    monitor = MonitorResult(samples_taken=40)
    monitor.all_lock_waits.extend([_attributable_event(), _attributable_event()])
    oltp = _summary("oltp_writer", ops=2000, rows=20_000)
    analytics = _summary("dagster_ingest", ops=500, rows=0)
    report = build_report(config, 4.0, oltp, analytics, monitor)
    assert report.passed is False
    assert report.load_floor_met is True
    assert report.analytics_attributable_lock_waits == 2
    assert "AC-1 FAIL" in report.verdict
    assert len(report.examples) == 2


def test_other_lock_waits_do_not_fail_ac1() -> None:
    # Writer-vs-writer contention is real but NOT analytics-attributable, so AC-1
    # still passes (this is the whole point of attribution).
    config = smoke_profile()
    monitor = MonitorResult(samples_taken=40)
    monitor.all_lock_waits.extend([_other_event(), _other_event(), _other_event()])
    oltp = _summary("oltp_writer", ops=2000, rows=20_000)
    analytics = _summary("dagster_ingest", ops=500, rows=0)
    report = build_report(config, 4.0, oltp, analytics, monitor)
    assert report.passed is True
    assert report.analytics_attributable_lock_waits == 0
    assert report.other_lock_waits == 3


def test_inconclusive_when_load_floor_not_met() -> None:
    config = smoke_profile()
    # Force below the floor with a tiny load over a long duration so achieved/day
    # is well under the smoke floor (10k).
    oltp = _summary("oltp_writer", ops=1, rows=1)
    duration = 100.0  # 1 row / 100s -> ~864/day, below the 10k floor
    analytics = _summary("dagster_ingest", ops=500, rows=0)
    report = build_report(config, duration, oltp, analytics, MonitorResult(samples_taken=40))
    assert report.passed is False
    assert report.load_floor_met is False
    assert "INCONCLUSIVE" in report.verdict


def test_event_attribution_property() -> None:
    assert _attributable_event().is_analytics_attributable is True
    assert _other_event().is_analytics_attributable is False
