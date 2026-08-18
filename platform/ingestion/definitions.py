"""Dagster ``Definitions`` for Component A — the object ``dagster dev`` loads.

This registers the FULL end-to-end analytical backbone as ONE connected asset
graph:

    raw ingestion (C2, Postgres -> DuckDB raw.raw_*)
        -> dbt medallion (C3, bronze -> silver/*_rejects -> gold)

The four raw assets and the dbt models share the same DuckDB file; dbt's
``source('raw', 'raw_*')`` nodes resolve to the SAME asset keys the ingestion
assets publish (``raw/raw_*``), so the dbt models appear DOWNSTREAM of ingestion
and a single "Materialize all" runs the pipeline end to end. See
``dbt_assets.py`` for the join-seam contract.

Load with::

    DAGSTER_HOME=... uv run dagster dev -m platform.ingestion.definitions

Resources:
  - ``postgres`` / ``duckdb_resource`` — bound to the C2 raw ingestion assets.
  - ``dbt`` — the :class:`DbtCliResource` the ``@dbt_assets`` step uses to run
    ``dbt build`` against the C3 project.

Automation:
  - ``backbone_end_to_end`` (job) — the named one-click end-to-end pipeline over
    the whole 18-asset graph. See ``jobs.py``.
  - ``backbone_every_15min`` (schedule) — 15-min cadence, STOPPED by default
    (operator-armed; mutates the warehouse). See ``schedules.py``.
  - ``backbone_failure_logger`` (sensor) — emits the I1 BACKBONE_RUN_FAILURE log
    line on any failed run, RUNNING by default (observes only). See ``sensors.py``.
"""

from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import ALL_ASSETS
from .dbt_assets import DBT_EXECUTABLE, dbt_medallion_assets, dbt_project
from .jobs import backbone_end_to_end
from .resources import DuckDBResource, PostgresResource
from .schedules import backbone_every_15min
from .sensors import backbone_failure_logger

defs = Definitions(
    assets=[*ALL_ASSETS, dbt_medallion_assets],
    jobs=[backbone_end_to_end],
    schedules=[backbone_every_15min],
    sensors=[backbone_failure_logger],
    resources={
        "postgres": PostgresResource(),
        "duckdb_resource": DuckDBResource(),
        # The dbt CLI resource is bound to the DbtProject so the @dbt_assets step
        # knows the project_dir/profiles_dir; it runs `dbt build` in-process.
        # dbt_executable is pinned to the active venv's dbt so it resolves even
        # when the bare `dbt` is not on PATH (e.g. under `dagster dev`).
        "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE),
    },
)
