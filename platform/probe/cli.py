"""C8 freshness probe CLI — the AC-3 gate entrypoint.

Drives a real source -> gold measurement (inject a beacon order, run C2 ingest,
run dbt build, time how long until the row is queryable in gold) and emits the
AC-3 verdict (median end-to-end lag <= 5 min).

Usage:
    uv run python -m platform.probe.cli --ci            # single-shot, JSON, CI
    uv run python -m platform.probe.cli --samples 3     # 3 samples, text report
    uv run python -m platform.probe.cli --json          # default samples, JSON

Exit code is the gate:
    0  -> AC-3 PASS (median lag <= budget)
    1  -> AC-3 FAIL (median lag > budget, or beacon never reached gold)
    2  -> precondition / pipeline error (could not run the gate)
   77  -> SKIPPED (source DB unreachable) — standard "skip" code for CI

A DB-down condition is a SKIP (77), NOT a failure: AC-3 cannot be measured
without the live pipeline, and silently passing/failing would be dishonest.
"""

from __future__ import annotations

import argparse
import sys
from platform.probe.freshness import (
    AC3_BUDGET_S,
    DEFAULT_SAMPLE_TIMEOUT_S,
    ProbeError,
    run_probe,
)
from platform.probe.verdict import build_report, render_text

import psycopg

# Conventional CI "skip" exit code (e.g. automake) — distinct from pass/fail.
EXIT_SKIPPED = 77


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="c8",
        description="C8 freshness probe — measures the AC-3 source->gold lag gate.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=3,
        help="number of inject->ingest->dbt->gold measurements; the MEDIAN is the "
        "AC-3 statistic. Default: 3.",
    )
    parser.add_argument(
        "--ci",
        action="store_true",
        help="single-shot CI mode: forces --samples 1 and --json. Fastest valid gate.",
    )
    parser.add_argument(
        "--budget-s",
        type=float,
        default=AC3_BUDGET_S,
        help=f"AC-3 median-lag budget in seconds. Default: {AC3_BUDGET_S:.0f} (5 min).",
    )
    parser.add_argument(
        "--sample-timeout-s",
        type=float,
        default=DEFAULT_SAMPLE_TIMEOUT_S,
        help="per-sample ceiling for the beacon to reach gold after dbt build. "
        f"Default: {DEFAULT_SAMPLE_TIMEOUT_S:.0f}.",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="leave injected beacon orders in the source (default: remove them).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the structured JSON report instead of the text report.",
    )
    return parser


def _db_reachable() -> tuple[bool, str]:
    """Return (reachable, reason). Used to convert DB-down into a clean SKIP."""
    from src.db.connection import conninfo

    try:
        with psycopg.connect(**conninfo(), connect_timeout=3) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True, ""
    except psycopg.Error as exc:
        return False, str(exc).strip()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    samples = 1 if args.ci else args.samples
    as_json = args.json or args.ci

    # SKIP-with-reason if the source DB is down: AC-3 is unmeasurable, not failed.
    reachable, reason = _db_reachable()
    if not reachable:
        msg = (
            "c8: SKIPPED — source Postgres is unreachable, so AC-3 (source->gold "
            f"freshness) cannot be measured. Reason: {reason}. "
            "Start it with `make up` (then `make seed`) and re-run."
        )
        print(msg, file=sys.stderr)
        return EXIT_SKIPPED

    try:
        run = run_probe(
            samples=samples,
            sample_timeout_s=args.sample_timeout_s,
            cleanup=not args.no_cleanup,
        )
    except ProbeError as exc:
        print(f"c8: cannot run AC-3 gate: {exc}", file=sys.stderr)
        return 2
    except psycopg.Error as exc:
        # The DB went away mid-run — treat as a skip, same rationale as above.
        print(
            f"c8: SKIPPED — lost connection to source Postgres mid-run: {exc}",
            file=sys.stderr,
        )
        return EXIT_SKIPPED

    report = build_report(run, budget_s=args.budget_s)

    if as_json:
        print(report.to_json())
    else:
        print(render_text(report))

    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
