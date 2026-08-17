# Reference · Run Log And Metadata

> **Scope:** The named-job + automation surface (`define_asset_job`,
> `ScheduleDefinition`, run-level concurrency config) and the structured run-log
> line the failure sensor emits — the I1 evidence the Sentinel reads.
> **No line limit.** Lookup material — completeness over brevity.

## Source of truth

Every claim below is traceable to one of these:

- `define_asset_job` + `AssetSelection.all()`:
  <https://docs.dagster.io/api/dagster/jobs> and
  <https://docs.dagster.io/api/dagster/asset-selection>
- `ScheduleDefinition` + `DefaultScheduleStatus`:
  <https://docs.dagster.io/api/dagster/schedules-sensors>
- Run-level op concurrency (`max_concurrent`):
  <https://docs.dagster.io/guides/operate/managing-concurrency>
- Verified against: dagster 1.13.10, dagster-dbt 0.29.10 (API introspected with
  `inspect.signature`), against the in-repo 18-asset graph.

## Reference

### `define_asset_job` (1.13.10 signature, key args)

| Arg | Type | Notes |
| --- | --- | --- |
| `name` | `str` | UI job name (shows in Jobs list). Required. |
| `selection` | `CoercibleToAssetSelection` | `AssetSelection.all()` = whole graph. |
| `config` | `Mapping`/`RunConfig`/`ConfigMapping`/`PartitionedConfig` | Baked-in run config. |
| `description`, `tags`, `run_tags`, `metadata`, `partitions_def`, `executor_def`, `hooks`, `op_retry_policy`, `owners` | — | optional. |

Returns an `UnresolvedAssetJobDefinition`. In `Definitions(jobs=[...])`, resolve
with `defs.resolve_job_def(name)` (1.11+); `defs.get_job_def(name)` still works but
warns. `job.asset_layer.executable_asset_keys` enumerates selected assets — used to
assert the backbone job selects all 18.

### Run-level serial execution (single-writer guard)

Bake into the job's `config` so the whole run executes ops one at a time
(honors the DuckDB single-writer rule):

```python
config = {"execution": {"config": {"multiprocess": {"max_concurrent": 1}}}}
backbone_end_to_end = define_asset_job(
    name="backbone_end_to_end",
    selection=AssetSelection.all(),
    config=config,
)
```

`multiprocess` is the default executor; `max_concurrent` caps ops-per-run. Default
limit is `multiprocessing.cpu_count()`, which would let the four raw assets fan out
onto the shared `.duckdb` file — `1` serializes them.

### `ScheduleDefinition` (key args)

| Arg | Type | Notes |
| --- | --- | --- |
| `name` | `str` | UI schedule name (distinct from `job.name`). |
| `job` | job def | The asset job to launch. |
| `cron_schedule` | `str` | Standard cron, e.g. `"*/15 * * * *"`. |
| `default_status` | `DefaultScheduleStatus` | `STOPPED` (operator-armed) / `RUNNING`. |

Convention in this repo: a schedule that MUTATES the warehouse defaults
`STOPPED` (R7 reversibility + single-writer); a pure-observer sensor defaults
`RUNNING`. See `patterns/sensor-and-run-status.md`.

### The I1 failure-log line (structured run log)

The failure sensor writes ONE line via `context.log.error`. Shape (stable
contract — the Log Analyst parses it):

```text
BACKBONE_RUN_FAILURE run_id=<uuid> job=<job_name> error=<failure message>
```

- Prefix `BACKBONE_RUN_FAILURE` is frozen — changing it breaks parsing.
- Fields: `run_id` (`context.dagster_run.run_id`), `job`
  (`context.dagster_run.job_name`), `error` (`context.failure_event.message`).

### Asset materialization metadata (raw ingestion)

`MaterializeResult(metadata=...)` from the raw assets carries (read back by the
next run's watermark logic and surfaced in the UI): `rows_read`, `rows_upserted`,
`raw_table_total`, `full_load`, `watermark_col`, `high_watermark_ts`,
`high_watermark_id`, `dagster/row_count`, and `schema_drift` (orders only). dbt
assets do not emit these keys, so any consumer must tolerate their absence
(`getattr(meta.get(k), "value", None)`).

## Cross-references

- `quick-reference.md` — this tech's index
- `patterns/sensor-and-run-status.md` — the sensor that writes the I1 line
- `platform/ingestion/jobs.py` / `schedules.py` / `sensors.py` — shipped code
