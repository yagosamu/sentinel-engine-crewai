# Pattern · Sensor And Run Status

> **Solves:** Emitting a deterministic, machine-parseable FAILURE signal when an
> orchestrated run fails — the I1 evidence surface the Sentinel's Log Analyst
> reads, without coupling Component A to Component B.
> **Limit:** ~200 lines. One reusable pattern, production-grade.

## Source of truth

- `run_failure_sensor` / `RunFailureSensorContext`:
  <https://docs.dagster.io/guides/automate/sensors/run-status-sensors>
- `monitored_jobs` (not `pipeline_selection`/`job_selection`):
  <https://docs.dagster.io/migration> (MIGRATION.md — `pipeline_selection` removed
  from `run_failure_sensor`; `job_selection` deprecated; use `monitored_jobs`)
- Registering sensors in `Definitions(sensors=[...])`:
  <https://docs.dagster.io/api/dagster/definitions>
- Verified against: dagster 1.13.10 (installed), API introspected via
  `inspect.signature(run_failure_sensor)`.

## Problem

The backbone runs as a named asset job (`backbone_end_to_end`). When a run fails,
the Sentinel (Component B) must be able to find WHICH run failed and WHY by reading
the structured run log read-only (interface I1) — no debugger, no DB poke. We need
A to WRITE a stable, grep-able failure line on every failed run, while never
importing or calling B (R3 one-way dependency).

## Pattern

Use `run_failure_sensor` (the specialization that fires ONLY on FAILURE and hands
you `RunFailureSensorContext` with `failure_event`), scoped to the job via
`monitored_jobs`, defaulting to RUNNING (it only observes/logs — must be armed to
be useful). Emit ONE line with a stable prefix + `key=value` fields.

```python
from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from .jobs import backbone_end_to_end

FAILURE_LOG_PREFIX = "BACKBONE_RUN_FAILURE"  # contract: parsed by the Log Analyst


@run_failure_sensor(
    name="backbone_failure_logger",
    monitored_jobs=[backbone_end_to_end],
    default_status=DefaultSensorStatus.RUNNING,
)
def backbone_failure_logger(context: RunFailureSensorContext) -> None:
    run = context.dagster_run
    context.log.error(
        "%s run_id=%s job=%s error=%s",
        FAILURE_LOG_PREFIX,
        run.run_id,
        run.job_name,
        context.failure_event.message,
    )
```

Register it:

```python
defs = Definitions(assets=[...], jobs=[...], schedules=[...], sensors=[backbone_failure_logger])
```

`RunFailureSensorContext` exposes (1.13.10): `dagster_run`, `failure_event`,
`dagster_event`, `log`, `instance`, `get_step_failure_events()`, `partition_key`,
`sensor_name`.

## Why this shape

- **`run_failure_sensor` over `run_status_sensor(run_status=FAILURE)`** — the
  former is purpose-built: fires only on failure and gives `failure_event`
  (structured `DagsterEvent` with `.message`) for free. The general form forces
  you to re-derive that and guard against non-failure statuses.
- **`monitored_jobs=[job]`** scopes to OUR job. `pipeline_selection` is REMOVED
  and `job_selection` deprecated — use `monitored_jobs`.
- **`default_status=RUNNING`** — the sensor is side-effect-free w.r.t. data, so
  default it ON; a failure signal you must remember to arm is silent exactly when
  needed. (Contrast: a schedule that MUTATES data defaults STOPPED — see
  `automation-job-schedule.md` reasoning / R7.)
- **Stable prefix + key=value** — the I1 contract. The Log Analyst greps
  `BACKBONE_RUN_FAILURE` and parses `run_id=`/`job=`/`error=`. Treat the prefix
  string as a frozen contract.

## Anti-patterns

- **Sensor triggers the Sentinel crew** — inverts the one-way dependency (R3). The
  sensor LOGS; B reads the log later. Never `import`/call B from A.
- **`run_status_sensor` with no status guard** acting on success too — re-runs the
  wrong branch (the docs warn about infinite loops when a success sensor launches
  a job whose success retriggers it).
- **Free-text log message** with no stable prefix — defeats deterministic parsing;
  the Log Analyst can't key on it.
- **`default_status=STOPPED` on a pure-observer sensor** — silent when you need it.

## Verify

- `dagster definitions validate -m platform.ingestion.definitions` loads it.
- `defs.get_sensor_def("backbone_failure_logger").default_status` == `RUNNING`.
- Force a failure run and grep the run log for `BACKBONE_RUN_FAILURE run_id=`.

## See also

- `quick-reference.md` — this tech's index
- `reference/run-log-and-metadata.md` — the log/metadata surface this writes to
- `platform/ingestion/sensors.py` — the shipped implementation
