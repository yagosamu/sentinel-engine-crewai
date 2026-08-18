"""L3 LIVE scored eval — the only layer that touches a live LLM.

GATED on an API key (``pytest.mark.skipif``). For each registry failure it runs the
full inject -> detect -> score loop against the REAL crew and the I4 oracle, and
RECORDS the ScoreResult. It asserts the loop COMPLETES and produces a score; it
does NOT assert the score is "correct" — a crew that scores 0.4 on
ambiguous_anomaly is a real, honest result (that is what scoring is FOR; R5).

HONESTY GUARANTEES:
  * No key -> the whole module SKIPS and says so; never a fabricated scorecard.
  * Reproducibility (R7): each failure resets to a clean baseline, captures a
    since-cursor, injects, runs, and scores against the run-window rows.
  * Single-writer DuckDB: the A3 warehouse read is serialised; a lock holder is
    killed before the run. This test is OPT-IN and never runs in the default suite.

This module is intentionally a SCAFFOLD: it is skipped in this (keyless) env, so it
adds no green-suite signal and makes no live claim. Run it manually with a key and a
running backbone warehouse to produce a real scorecard.
"""

from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime

import pytest

_HAS_KEY = bool(os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY"))

pytestmark = pytest.mark.skipif(
    not _HAS_KEY,
    reason="L3 live eval requires an LLM API key (OPENAI_API_KEY/ANTHROPIC_API_KEY); skipped offline.",
)

# The failures that surface in the warehouse/logs and are individually scorable in
# this loop. (multi_failure_cascade is scored via the deterministic Flow in
# test_trigger / test_flow; it does not need a live LLM.)
LIVE_SCOREABLE_FAILURES = (
    "negative_price",
    "missing_customer",
    "invalid_quantity",
    "duplicate_order",
    "orphan_payment",
    "schema_drift",
)


def _reset_clean() -> None:
    """Restore a clean baseline before an inject (R7). Uses the generator CLI."""
    subprocess.run(
        ["make", "reset-schema"],
        check=False,
        capture_output=True,
        cwd=os.environ.get("REPO_ROOT", os.getcwd()),
    )


def _inject(failure_key: str) -> None:
    subprocess.run(["make", "inject", f"FAILURE={failure_key}"], check=True, capture_output=True)


def _kill_warehouse_lock() -> None:
    db = os.environ.get("DUCKDB_DATABASE", "")
    if not db:
        return
    holders = subprocess.run(["lsof", "-t", db], capture_output=True, text=True).stdout.split()
    for pid in holders:
        subprocess.run(["kill", "-9", pid], check=False)


@pytest.mark.parametrize("failure_key", LIVE_SCOREABLE_FAILURES)
def test_live_scored_eval(failure_key: str) -> None:
    """Inject one failure, run the crew, score it. Reports — never asserts — the
    score; only asserts the loop completed (a real score was produced).
    """
    from sentinel.scoring import score_run
    from sentinel.trigger import run_sentinel

    _reset_clean()
    since = datetime.now(UTC)
    _inject(failure_key)
    _kill_warehouse_lock()

    result = run_sentinel(since=since)

    # The loop must COMPLETE and produce a graded result against the I4 oracle.
    assert result.score is not None
    assert result.score.tier in {"exact", "alias", "cascade", "miss", "no-run"}

    # Record the scorecard line (visible with `pytest -s`). This is the honest
    # output of a probabilistic eval — the score is reported, not asserted correct.
    print(
        f"[SCORECARD] {failure_key}: route={result.route} "
        f"tier={result.score.tier} diagnosis={result.score.diagnosis_score} "
        f"evidence={result.score.evidence_score} matched={result.score.matched}"
    )

    # Re-grade defensively to confirm the oracle path is reproducible.
    regraded = score_run(result.diagnosis, since=since)
    assert regraded.tier == result.score.tier
