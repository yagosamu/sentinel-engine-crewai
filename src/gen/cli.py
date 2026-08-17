"""``argparse`` front-end for the chaos generator (``python -m src.gen.cli``).

Subcommands ``list`` / ``traffic`` / ``inject`` / ``reset-schema`` / ``watch``
back the Makefile targets ``failures`` / ``traffic`` / ``inject`` / others. Each
handler returns a process exit code.
"""

from __future__ import annotations

import argparse
import sys

from src.gen import engine
from src.gen import repository as repo
from src.gen.failures import REGISTRY


def _list(_: argparse.Namespace) -> int:
    width = max(len(key) for key in REGISTRY)
    base = sorted(k for k, f in REGISTRY.items() if f.unlocks.startswith("base crew"))
    advanced = sorted(k for k, f in REGISTRY.items() if not f.unlocks.startswith("base crew"))

    print("\nBASE CREW failures (detect / diagnose / report):")
    for key in base:
        failure = REGISTRY[key]
        print(f"  {key.ljust(width)}  [{failure.detected_by}]  {failure.summary}")

    print("\nFEATURE-UNLOCKING failures (each demands a CrewAI capability):")
    for key in advanced:
        failure = REGISTRY[key]
        print(f"  {key.ljust(width)}  [{failure.detected_by}]  {failure.summary}")
        print(f"  {' '.ljust(width)}  -> unlocks {failure.unlocks}")
    return 0


def _traffic(args: argparse.Namespace) -> int:
    with repo.session() as conn:
        inserted = engine.run_traffic(conn, args.orders)
    print(f"inserted {inserted:,} orders")
    return 0


def _inject(args: argparse.Namespace) -> int:
    with repo.session() as conn:
        result = engine.inject(conn, args.failure)
    print(f"injected {result.failure}: {result.detail}  (detected by {result.detected_by})")
    return 0


def _reset_schema(_: argparse.Namespace) -> int:
    """Revert ONLY the ``schema_drift`` column rename (user_id -> customer_id).

    This is the sole self-reversing remediation in the layer. It does NOT
    restore constraints dropped by other injectors or undo row mutations, so a
    true clean baseline still requires a full rebuild + reseed (R7).
    """
    with repo.session() as conn:
        column = repo.order_customer_column(conn)
        if column == "user_id":
            repo.execute(conn, "ALTER TABLE orders RENAME COLUMN user_id TO customer_id")
            conn.commit()
            print("reverted orders.user_id -> customer_id")
        else:
            print("orders.customer_id already correct")
    return 0


def _watch(args: argparse.Namespace) -> int:
    with repo.session() as conn:
        try:
            engine.watch(
                conn,
                interval=args.interval,
                batch=args.batch,
                failure_every=args.failure_every,
                failures=args.failures or [],
                on_event=lambda message: print(message, flush=True),
            )
        except KeyboardInterrupt:
            print("\nstopped")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the ``gen`` argument parser with all five subcommands wired up."""
    parser = argparse.ArgumentParser(prog="gen", description="E-commerce traffic and failure generator.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list available failure modes").set_defaults(func=_list)

    traffic = sub.add_parser("traffic", help="insert normal orders")
    traffic.add_argument("--orders", type=int, default=200)
    traffic.set_defaults(func=_traffic)

    inject = sub.add_parser("inject", help="inject a single failure mode")
    inject.add_argument("failure", choices=sorted(REGISTRY))
    inject.set_defaults(func=_inject)

    sub.add_parser("reset-schema", help="revert schema drift (user_id -> customer_id)").set_defaults(func=_reset_schema)

    watch = sub.add_parser("watch", help="continuously stream traffic and inject failures")
    watch.add_argument("--interval", type=float, default=3.0)
    watch.add_argument("--batch", type=int, default=50)
    watch.add_argument("--failure-every", type=int, default=5)
    watch.add_argument("--failures", nargs="*", choices=sorted(REGISTRY))
    watch.set_defaults(func=_watch)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse ``argv`` and dispatch to the chosen subcommand handler."""
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
