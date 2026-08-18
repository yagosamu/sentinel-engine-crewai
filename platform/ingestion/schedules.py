"""The backbone schedule — operator-armed 15-minute cadence.

Defines a single :class:`ScheduleDefinition` that runs :data:`backbone_end_to_end`
every 15 minutes. It imports the job object from :mod:`jobs` so it binds the SAME
instance the failure sensor monitors.

CADENCE: ``*/15 * * * *`` (every 15 min). Tied to AC-3 (freshness <= 5 min) as
the OUTER bound that keeps the pipeline warm and demonstrable WITHOUT claiming
AC-3 compliance by itself — AC-3 is a freshness BUDGET proven by the C8 freshness
probe, not by schedule frequency. 15 min leaves head-room for the run's own
duration; do NOT tighten to */5 — a run that overruns 5 min would overlap itself
and collide on the single DuckDB writer.

DEFAULT STATUS: STOPPED. The schedule MUTATES the warehouse, so it must be
operator-armed, not auto-armed:

  - R7 (chaos is reversible): inject -> detect -> score runs restore a clean
    baseline first. An auto-running schedule firing every 15 min would race the
    eval harness, mutate the warehouse mid-eval, and make scoring
    non-reproducible.
  - Single-writer: an always-on schedule PLUS a manual one-click run PLUS an
    eval run = concurrent writers to one .duckdb. STOPPED removes that class of
    accident by default.

The operator flips it on in the UI Schedules tab (one click) to enable the
15-min loop when cadence is wanted.
"""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

from .jobs import backbone_end_to_end

# The schedule's UI name (distinct from the job name it targets).
BACKBONE_SCHEDULE_NAME = "backbone_every_15min"

# Every 15 minutes. The OUTER freshness bound (see module docstring / AC-3).
BACKBONE_CRON = "*/15 * * * *"

backbone_every_15min = ScheduleDefinition(
    name=BACKBONE_SCHEDULE_NAME,
    job=backbone_end_to_end,
    cron_schedule=BACKBONE_CRON,
    # STOPPED by default: mutates the warehouse; operator arms it in the UI.
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Runs backbone_end_to_end every 15 minutes. STOPPED by default "
        "(operator-armed) so it never races the chaos-eval harness or the "
        "single DuckDB writer; flip ON in the UI when 15-min cadence is wanted."
    ),
)
