"""Plain-Python materialize entrypoint for the end-to-end backbone.

Lets tests and ``make ingest-once`` run the FULL pipeline WITHOUT the Dagster
webserver. Thin wrapper over :func:`dagster.materialize` that binds the same
three resources as :mod:`platform.ingestion.definitions` and, by default,
materializes the WHOLE 18-asset graph: C2 raw ingestion (Postgres -> DuckDB
``raw.raw_*``) THEN the C3 dbt medallion (bronze -> silver/*_rejects -> gold).

INSTANCE / WATERMARK PERSISTENCE
--------------------------------
Incremental extraction reads the prior high-watermark from the LAST successful
materialization (``context.instance.fetch_materializations``). For that to work
across runs the materialization events must be persisted, which requires a
DagsterInstance backed by storage:

  - ``materialize_ingest(instance=...)`` — pass an explicit instance (a persistent
    or temporary one). This is what unit tests use to assert incremental behaviour
    within a single test.
  - CLI ``--persistent`` — use the ``DAGSTER_HOME`` instance (same store as
    ``dagster dev``), so successive CLI runs are genuinely incremental.
  - default CLI / ``instance=None`` — an EPHEMERAL instance: every run is a cold
    start (full load). Idempotent because raw upserts by PK, so re-running is safe;
    it just re-scans everything.

INVOCATION (both work)
----------------------
::

    uv run python -m platform.ingestion.run            # module: ephemeral, full graph
    uv run python -m platform.ingestion.run --persistent  # module: incremental via DAGSTER_HOME
    uv run python platform/ingestion/run.py            # bare script (from repo root)

The bare-script form requires the local ``platform`` package (which shadows the
stdlib ``platform`` module) to resolve, so the shim below re-anchors ``sys.path``
to the repo root. The Makefile's ``PYTHONPATH=<repo>`` contract is the primary,
reliable path; treat the bare-script invocation as a convenience.

SINGLE-WRITER: :func:`dagster.materialize` runs in-process and executes the
graph serially in topological order, so the four raw assets and the dbt step
never write the shared ``.duckdb`` file concurrently.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

# --------------------------------------------------------------------------- #
# Dual-mode import shim.                                                        #
#                                                                              #
# Relative imports (`.assets`) require package context, which exists under     #
# `python -m platform.ingestion.run` but NOT under `python                     #
# platform/ingestion/run.py` (bare script => __package__ is None/"" ). When    #
# run as a bare script we insert the repo root on sys.path and switch to       #
# absolute imports of the local `platform` package. NB: `platform` here is the #
# repo's own package, which shadows the stdlib module — parents[2] is the repo #
# root (this file is <repo>/platform/ingestion/run.py).                        #
# --------------------------------------------------------------------------- #
if __package__ in (None, ""):  # bare-script invocation
    import pathlib

    _REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))

    from platform.ingestion.assets import ALL_ASSETS
    from platform.ingestion.dbt_assets import (
        DBT_EXECUTABLE,
        dbt_medallion_assets,
        dbt_project,
    )
    from platform.ingestion.resources import DuckDBResource, PostgresResource

    from dagster import DagsterInstance, ExecuteInProcessResult, materialize
    from dagster_dbt import DbtCliResource
else:  # `python -m platform.ingestion.run` (package context present)
    from dagster import DagsterInstance, ExecuteInProcessResult, materialize
    from dagster_dbt import DbtCliResource

    from .assets import ALL_ASSETS
    from .dbt_assets import DBT_EXECUTABLE, dbt_medallion_assets, dbt_project
    from .resources import DuckDBResource, PostgresResource

if TYPE_CHECKING:
    from dagster import AssetsDefinition


# The FULL end-to-end target — mirrors definitions.py's assets=[...] exactly:
# the four raw @asset nodes PLUS the dbt medallion @dbt_assets. Materializing
# this list runs ingest -> dbt build as one in-process, topologically-ordered run.
ALL_BACKBONE_ASSETS = [*ALL_ASSETS, dbt_medallion_assets]


def _resources() -> dict[str, object]:
    """The three resources the full graph needs (matches definitions.py)."""
    return {
        "postgres": PostgresResource(),
        "duckdb_resource": DuckDBResource(),
        # The dbt step needs the DbtCliResource bound to the same project_dir /
        # executable as definitions.py, or the dbt assets cannot run.
        "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE),
    }


def materialize_ingest(
    *,
    assets: list[AssetsDefinition] | None = None,
    instance: DagsterInstance | None = None,
) -> ExecuteInProcessResult:
    """Materialize the backbone graph in-process and return the result.

    Args:
        assets: Subset of assets to materialize. Default is the FULL 18-asset
            graph (four raw assets + the dbt medallion) so this is a genuine
            end-to-end run. Tests pass an explicit subset (e.g. a single raw
            asset) to exercise one entity in isolation; that path is untouched,
            so incremental-watermark unit tests keep working.
        instance: DagsterInstance to record events against. When ``None`` an
            ephemeral instance is created (cold start every run). Pass a
            persistent/temporary instance to get incremental watermark behaviour.

    Returns:
        The :class:`dagster.ExecuteInProcessResult`; ``.success`` is the gate and
        ``.get_asset_materialization_events()`` exposes the emitted metadata.
    """
    target = assets if assets is not None else ALL_BACKBONE_ASSETS
    return materialize(target, resources=_resources(), instance=instance)


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the end-to-end backbone (Postgres -> DuckDB raw.raw_* -> dbt medallion).",
    )
    parser.add_argument(
        "--persistent",
        action="store_true",
        help="Use the DAGSTER_HOME instance so successive runs are incremental "
        "(default: ephemeral instance => full load each run).",
    )
    args = parser.parse_args(argv)

    if args.persistent:
        with DagsterInstance.get() as instance:
            result = materialize_ingest(instance=instance)
    else:
        result = materialize_ingest()

    for entry in result.get_asset_materialization_events():
        mat = entry.event_specific_data.materialization
        meta = mat.metadata or {}
        # raw assets carry rows_read / raw_table_total; dbt assets do not, so
        # getattr(..., None) tolerates their absence and prints None counts.
        total = meta.get("raw_table_total")
        read = meta.get("rows_read")
        total_val = getattr(total, "value", None)
        read_val = getattr(read, "value", None)
        print(f"{'/'.join(mat.asset_key.path)}: rows_read={read_val} raw_table_total={total_val}")

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(_main())
