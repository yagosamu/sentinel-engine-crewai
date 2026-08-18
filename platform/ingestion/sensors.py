"""The backbone failure sensor — the I1 evidence surface for the Sentinel.

Defines a :func:`run_failure_sensor` that fires when :data:`backbone_end_to_end`
fails and writes ONE stable, grep-able, key=value failure line to the structured
run log. That line is the I1 evidence surface the Sentinel's Log Analyst reads —
a deterministic, machine-parseable failure record, no debugger required.

WHY ``run_failure_sensor`` (not a general ``run_status_sensor``)
---------------------------------------------------------------
``run_failure_sensor`` is the purpose-built specialization: it fires ONLY on
FAILURE and hands the body a :class:`RunFailureSensorContext` carrying
``failure_event`` (the structured DagsterEvent with the error message) for free.
A general ``run_status_sensor(run_status=FAILURE)`` would work but would
re-derive that and require guarding against non-failure statuses. For "emit a
legible failure log," ``run_failure_sensor`` is the idiomatic minimal choice.

ONE-WAY DEPENDENCY (R3)
-----------------------
This sensor only WRITES the log. Component B reads it read-only LATER. It does
NOT import, call, or trigger Component B — doing so would invert the one-way
dependency (A never depends on B). If anyone proposes the sensor trigger the
Sentinel crew, reject it: the sensor logs, the Sentinel reads.

DEFAULT STATUS: RUNNING. Unlike the schedule (which mutates the warehouse and
must default OFF), this sensor only OBSERVES and LOGS — it is side-effect-free
w.r.t. data and reversibility. A failure signal you must remember to arm is
silent exactly when you need it, so it defaults ON: any failed run (manual or
scheduled) leaves a legible trace immediately.
"""

from __future__ import annotations

from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    run_failure_sensor,
)

from .jobs import backbone_end_to_end

# The sensor's UI name.
BACKBONE_FAILURE_SENSOR_NAME = "backbone_failure_logger"

# The stable, grep-able prefix the Sentinel's Log Analyst keys on (I1 surface).
# Treat this string as a contract: changing it silently breaks log-side parsing.
FAILURE_LOG_PREFIX = "BACKBONE_RUN_FAILURE"


@run_failure_sensor(
    name=BACKBONE_FAILURE_SENSOR_NAME,
    monitored_jobs=[backbone_end_to_end],
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Emits a stable BACKBONE_RUN_FAILURE key=value log line whenever "
        "backbone_end_to_end fails — the I1 evidence surface the Sentinel's "
        "Log Analyst reads. Logs only; never imports or triggers Component B."
    ),
)
def backbone_failure_logger(context: RunFailureSensorContext) -> None:
    """Write a single structured failure line to the run log on any failed run.

    The line is the I1 evidence surface: a stable ``BACKBONE_RUN_FAILURE`` prefix
    followed by ``key=value`` fields (``run_id``, ``job``, ``error``) so the
    Sentinel's Log Analyst can parse it deterministically without a debugger.
    """
    run = context.dagster_run
    context.log.error(
        "%s run_id=%s job=%s error=%s",
        FAILURE_LOG_PREFIX,
        run.run_id,
        run.job_name,
        context.failure_event.message,
    )
