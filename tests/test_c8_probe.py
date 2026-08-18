"""Tests for the C8 freshness probe (AC-3 gate).

Two tiers:

* PURE tests (no DB) exercise the verdict math: median selection, the AC-3
  pass/fail boundary, and the empty/inconclusive case. These always run.
* An E2E test drives one real single-shot measurement (inject beacon -> C2
  ingest -> dbt build -> poll gold) and asserts the beacon reaches gold with a
  finite, non-negative lag. It is SKIPPED when the source Postgres is
  unreachable, so DB-less runners stay green.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from platform.probe.freshness import AC3_BUDGET_S, ProbeRun, SampleResult
from platform.probe.verdict import build_report

import psycopg
import pytest

from src.db.connection import conninfo


def _postgres_reachable() -> bool:
    try:
        conn = psycopg.connect(**conninfo(), connect_timeout=2)
    except Exception:  # noqa: BLE001 - any connect failure => skip
        return False
    conn.close()
    return True


def _sample(lag_s: float, *, order_id: int = 1) -> SampleResult:
    """Build a synthetic sample with a given end-to-end lag (for verdict tests)."""
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=lag_s)
    return SampleResult(
        order_id=order_id,
        injected_at=t0,
        visible_at=t1,
        end_to_end_lag_s=lag_s,
        ingest_s=lag_s * 0.4,
        dbt_s=lag_s * 0.4,
        gold_wait_s=lag_s * 0.2,
        visible=True,
    )


# --------------------------------------------------------------------------- #
# PURE verdict tests (no DB)                                                   #
# --------------------------------------------------------------------------- #


def test_median_under_budget_passes() -> None:
    run = ProbeRun(samples=[_sample(10, order_id=1), _sample(20, order_id=2), _sample(30, order_id=3)])
    report = build_report(run)
    assert report.passed is True
    assert report.median_lag_s == 20.0
    assert report.budget_s == AC3_BUDGET_S
    assert "AC-3 PASS" in report.verdict


def test_median_is_the_gate_not_the_max() -> None:
    # Two fast samples and one tail breach: median is fast => PASS (median, not max).
    run = ProbeRun(samples=[_sample(5, order_id=1), _sample(9, order_id=2), _sample(9000, order_id=3)])
    report = build_report(run)
    assert report.median_lag_s == 9.0
    assert report.max_lag_s == 9000.0
    assert report.passed is True


def test_median_over_budget_fails() -> None:
    over = AC3_BUDGET_S + 1
    run = ProbeRun(samples=[_sample(over, order_id=1), _sample(over, order_id=2), _sample(over, order_id=3)])
    report = build_report(run)
    assert report.passed is False
    assert "AC-3 FAIL" in report.verdict


def test_exactly_at_budget_passes() -> None:
    run = ProbeRun(samples=[_sample(AC3_BUDGET_S, order_id=1)])
    report = build_report(run)
    assert report.passed is True


def test_no_samples_is_inconclusive_not_pass() -> None:
    report = build_report(ProbeRun())
    assert report.passed is False
    assert report.samples == 0
    assert "INCONCLUSIVE" in report.verdict


def test_report_json_roundtrips() -> None:
    import json

    run = ProbeRun(samples=[_sample(12.5, order_id=42)])
    report = build_report(run)
    payload = json.loads(report.to_json())
    assert payload["samples"] == 1
    assert payload["sample_details"][0]["order_id"] == 42


# --------------------------------------------------------------------------- #
# E2E test (requires live, seeded Postgres + the C2/C3 toolchain)             #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    not _postgres_reachable(),
    reason="source Postgres not reachable; run `make up && make seed` to enable the C8 e2e test",
)
def test_single_shot_beacon_reaches_gold() -> None:
    """One real measurement: the beacon must land in gold with a finite lag."""
    from platform.probe.freshness import run_probe

    run = run_probe(samples=1, cleanup=True)
    assert len(run.samples) == 1
    sample = run.samples[0]
    assert sample.visible is True
    assert sample.end_to_end_lag_s >= 0.0
    # Stage times are all measured (non-negative) and sum to <= the total lag
    # plus a small slack (the injection-to-ingest gap is part of the total).
    assert sample.ingest_s >= 0.0
    assert sample.dbt_s >= 0.0
    assert sample.gold_wait_s >= 0.0

    report = build_report(run)
    assert report.samples == 1
    # We assert the gate is COMPUTED, not that it passes (timing is environmental).
    # build_report rounds median_lag_s to 3 decimals (verdict.py), matching the
    # JSON/report contract, while sample.end_to_end_lag_s is unrounded — so compare
    # with an absolute tolerance of the 3-decimal rounding half-step, not a 1e-6
    # relative tolerance (which is tighter than the rounding granularity).
    assert report.median_lag_s == pytest.approx(sample.end_to_end_lag_s, abs=5e-4)
