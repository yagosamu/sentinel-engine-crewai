"""C4h harness configuration (AC-1 gate).

A single immutable :class:`HarnessConfig` carries every knob the harness needs.
Two named profiles exist:

* ``full``  — the real AC-1 gate: 75k-orders/day-equivalent insert load for a
  meaningful window, against a representative concurrent analytics read load.
* ``smoke`` — a small/fast variant for CI and local sanity checks. Same code
  path, same attribution logic, same verdict shape — just fewer rows and a
  short window so it runs in a couple of seconds.

The 75k/day target is converted to a steady **orders-per-second** rate so the
harness is duration-independent: a 60s ``full`` run and a 5s ``smoke`` run both
drive the same *intensity*, only for different lengths of time.

ATTRIBUTION LINCHPIN (the AC-1 contract):
  Every Postgres session opened by the harness is tagged with an
  ``application_name`` so ``pg_stat_activity`` can attribute lock-waits:

  * transactional writers -> ``oltp_writer``  (the path that MUST stay clear)
  * analytics readers      -> ``dagster_ingest`` (the analytics surface; the
    contract's AC-1 attribution name — any lock-wait it *causes* on the
    transactional path fails the gate)
"""

from __future__ import annotations

from dataclasses import dataclass

# The AC-1 workload target, expressed as a daily order volume. Converted to a
# steady per-second insert rate so any run duration drives the same intensity.
ORDERS_PER_DAY_TARGET = 75_000
SECONDS_PER_DAY = 86_400

# application_name tags — the linchpin of AC-1 attribution. Do not change these
# without updating the contract: 'dagster_ingest' is the analytics attribution
# name C2/the analytics path uses fleet-wide.
OLTP_APP_NAME = "oltp_writer"
ANALYTICS_APP_NAME = "dagster_ingest"


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    """Immutable run configuration for one AC-1 harness execution."""

    profile: str
    # AC-1 validity floor: the run must SUSTAIN at least this many orders/day-
    # equivalent for the verdict to count as a real peak-load test. (A gate that
    # passed under no load would be meaningless.) Set to the 75k/day target.
    min_orders_per_day: int
    # Wall-clock seconds to sustain the load.
    duration_s: float
    # Number of concurrent transactional writer sessions.
    oltp_workers: int
    # Number of concurrent analytics reader sessions.
    analytics_workers: int
    # Orders per INSERT batch on the transactional path (one commit per batch).
    oltp_batch_size: int
    # How often the monitor samples pg_locks / pg_stat_activity, in seconds.
    sample_interval_s: float
    # statement_timeout for transactional writer sessions, in milliseconds.
    # Must be < the run window so a stuck commit cannot outlive the test and
    # keeps AC-1 interpretable.
    oltp_statement_timeout_ms: int
    # statement_timeout for analytics sessions, in milliseconds.
    analytics_statement_timeout_ms: int
    # PEAK MODE: writers run UNTHROTTLED to saturate the OLTP path — this is what
    # makes it a "peak-load" test (the harness reports achieved throughput and
    # checks it cleared min_orders_per_day). When False, writers are throttled to
    # exactly ``min_orders_per_day`` (a steady-state, not peak, run).
    peak_mode: bool = True
    # The AC-1 pass condition is "zero analytics-attributable lock-waits". This
    # tolerance lets CI absorb at most N transient samples (default 0 = strict).
    max_analytics_lock_waits: int = 0

    @property
    def throttled_orders_per_second(self) -> float:
        """Steady insert rate realising ``min_orders_per_day`` (throttled mode)."""
        return self.min_orders_per_day / SECONDS_PER_DAY


def full_profile() -> HarnessConfig:
    """The real AC-1 gate: peak (saturating) OLTP load for 60s, real concurrency.

    Writers run unthrottled to apply genuine peak pressure; the run must clear
    the 75k/day-equivalent floor AND show zero analytics-attributable lock-wait.
    """
    return HarnessConfig(
        profile="full",
        min_orders_per_day=ORDERS_PER_DAY_TARGET,
        duration_s=60.0,
        oltp_workers=4,
        analytics_workers=3,
        oltp_batch_size=25,
        sample_interval_s=0.25,
        oltp_statement_timeout_ms=5_000,
        analytics_statement_timeout_ms=30_000,
        peak_mode=True,
        max_analytics_lock_waits=0,
    )


def smoke_profile() -> HarnessConfig:
    """Small/fast CI variant: same peak-saturation code path, ~4s window.

    The floor is scaled down so a 4s run on a small CI box still 'counts' — CI
    proves the attribution machinery and isolation hold under real concurrency,
    just for a short window.
    """
    return HarnessConfig(
        profile="smoke",
        # A 4s smoke on a laptop comfortably exceeds a few thousand orders; keep
        # the floor modest so the *validity* check is meaningful but not flaky.
        min_orders_per_day=10_000,
        duration_s=4.0,
        oltp_workers=2,
        analytics_workers=2,
        oltp_batch_size=10,
        sample_interval_s=0.1,
        oltp_statement_timeout_ms=5_000,
        analytics_statement_timeout_ms=10_000,
        peak_mode=True,
        max_analytics_lock_waits=0,
    )


PROFILES = {"full": full_profile, "smoke": smoke_profile}


def get_profile(name: str) -> HarnessConfig:
    """Return the named profile config, or raise for an unknown name."""
    try:
        return PROFILES[name]()
    except KeyError:
        valid = ", ".join(sorted(PROFILES))
        raise ValueError(f"unknown profile {name!r}; valid profiles: {valid}") from None
