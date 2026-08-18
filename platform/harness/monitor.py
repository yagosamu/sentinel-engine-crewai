"""Lock-wait monitor for AC-1 attribution.

Runs in a background thread, polling Postgres at ``sample_interval_s``. On each
tick it asks the server which backends are *blocked waiting on a lock* and, for
each, which backends are *blocking* them (via the built-in ``pg_blocking_pids``).
It then attributes the wait by the blocker's ``application_name``.

AC-1 cares about exactly one event class:

    a transactional-path session (application_name = 'oltp_writer') that is
    waiting on a lock held by an analytics session (application_name =
    'dagster_ingest').

Every such observation is recorded as a :class:`LockWaitEvent`. The verdict
layer counts these: zero => AC-1 PASS.

We also record the broader picture (any lock-wait, regardless of attribution)
so the report can distinguish "no contention at all" from "contention, but not
analytics-attributable".
"""

from __future__ import annotations

import contextlib
import threading
import time
from dataclasses import dataclass, field
from platform.harness.config import ANALYTICS_APP_NAME, OLTP_APP_NAME
from platform.harness.pg_session import monitor_session

import psycopg

# One query that returns, for every backend currently waiting on a Lock, the
# blocker it is waiting behind and both application_names. pg_blocking_pids()
# is the authoritative "who is blocking pid X" function (handles transitive
# and group-locking cases the naive pg_locks self-join misses).
_BLOCKING_SQL = """
SELECT
    waiter.pid              AS waiter_pid,
    waiter.application_name AS waiter_app,
    waiter.wait_event_type  AS waiter_wait_type,
    waiter.wait_event       AS waiter_wait_event,
    blocker.pid             AS blocker_pid,
    blocker.application_name AS blocker_app,
    left(waiter.query, 200)  AS waiter_query
FROM pg_stat_activity AS waiter
JOIN LATERAL unnest(pg_blocking_pids(waiter.pid)) AS blocking(pid) ON TRUE
JOIN pg_stat_activity AS blocker ON blocker.pid = blocking.pid
WHERE waiter.wait_event_type = 'Lock'
  AND cardinality(pg_blocking_pids(waiter.pid)) > 0
"""


@dataclass(frozen=True, slots=True)
class LockWaitEvent:
    """One observed lock-wait edge (a waiter blocked by a blocker)."""

    sampled_at: float
    waiter_pid: int
    waiter_app: str
    blocker_pid: int
    blocker_app: str
    wait_event_type: str
    wait_event: str | None
    waiter_query: str

    @property
    def is_analytics_attributable(self) -> bool:
        """True iff a transactional writer is blocked by an analytics session.

        This is the precise AC-1 failure condition.
        """
        return self.waiter_app == OLTP_APP_NAME and self.blocker_app == ANALYTICS_APP_NAME


@dataclass(slots=True)
class MonitorResult:
    """All lock-wait edges observed across a run, with sampling counts.

    Splits the edges into ``analytics_attributable`` (the AC-1 failure set) and
    ``other_lock_waits`` (writer-vs-writer, which does not fail AC-1).
    """

    samples_taken: int = 0
    sample_errors: int = 0
    all_lock_waits: list[LockWaitEvent] = field(default_factory=list)

    @property
    def analytics_attributable(self) -> list[LockWaitEvent]:
        return [e for e in self.all_lock_waits if e.is_analytics_attributable]

    @property
    def other_lock_waits(self) -> list[LockWaitEvent]:
        return [e for e in self.all_lock_waits if not e.is_analytics_attributable]


class LockWaitMonitor:
    """Background sampler of analytics-attributable lock-waits."""

    def __init__(self, sample_interval_s: float) -> None:
        self._interval = sample_interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.result = MonitorResult()

    def _sample(self, conn: psycopg.Connection) -> None:
        now = time.time()
        with conn.cursor() as cur:
            cur.execute(_BLOCKING_SQL)
            for row in cur.fetchall():
                (
                    waiter_pid,
                    waiter_app,
                    waiter_wait_type,
                    waiter_wait_event,
                    blocker_pid,
                    blocker_app,
                    waiter_query,
                ) = row
                self.result.all_lock_waits.append(
                    LockWaitEvent(
                        sampled_at=now,
                        waiter_pid=waiter_pid,
                        waiter_app=waiter_app or "",
                        blocker_pid=blocker_pid,
                        blocker_app=blocker_app or "",
                        wait_event_type=waiter_wait_type or "",
                        wait_event=waiter_wait_event,
                        waiter_query=waiter_query or "",
                    )
                )

    def _run(self) -> None:
        conn = monitor_session()
        try:
            while not self._stop.is_set():
                try:
                    self._sample(conn)
                    self.result.samples_taken += 1
                except psycopg.Error:
                    # A transient sampling error must never abort the gate; count
                    # it and keep going on a fresh connection.
                    self.result.sample_errors += 1
                    with contextlib.suppress(psycopg.Error):
                        conn.close()
                    conn = monitor_session()
                self._stop.wait(self._interval)
        finally:
            with contextlib.suppress(psycopg.Error):
                conn.close()

    def __enter__(self) -> LockWaitMonitor:
        self._thread = threading.Thread(target=self._run, name="c4h-lockwait-monitor", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
