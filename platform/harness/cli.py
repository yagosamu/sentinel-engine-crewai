"""C4h harness CLI — the AC-1 gate entrypoint.

Usage:
    uv run python -m platform.harness.cli --profile smoke
    uv run python -m platform.harness.cli --profile full --json

Exit code is the gate:
    0  -> AC-1 PASS (peak load applied AND no analytics-attributable lock-wait)
    1  -> AC-1 FAIL  (an OLTP writer was blocked by an analytics session)
    2  -> precondition / environment error (could not run the gate)
    3  -> AC-1 INCONCLUSIVE (load floor not met; not a valid peak-load test)
"""

from __future__ import annotations

import argparse
import sys
from platform.harness.config import PROFILES, get_profile
from platform.harness.runner import PreconditionError, run
from platform.harness.verdict import render_text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="c4h",
        description="C4h peak-load harness — measures the AC-1 isolation gate.",
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="smoke",
        help="workload profile: 'full' (75k/day, 60s real gate) or 'smoke' (fast CI). Default: smoke.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="override the profile's run window in seconds.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the structured JSON report instead of the text report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = get_profile(args.profile)
    if args.duration is not None:
        from dataclasses import replace

        config = replace(config, duration_s=args.duration)

    try:
        report = run(config)
    except PreconditionError as exc:
        print(f"c4h: cannot run AC-1 gate: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(report.to_json())
    else:
        print(render_text(report))

    if report.passed:
        return 0
    # Distinguish a real failure (contention) from an invalid test (no load).
    return 1 if report.load_floor_met else 3


if __name__ == "__main__":
    sys.exit(main())
