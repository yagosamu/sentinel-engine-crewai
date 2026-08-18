"""The named end-to-end backbone job — Component A's one-click pipeline.

This module defines a SINGLE asset job over the WHOLE connected graph so the
ingest -> dbt medallion pipeline appears as one named, runnable job in the
Dagster UI Jobs list (not just the implicit "Materialize all" affordance) and
leaves a single SUCCESS row on the Runs timeline.

WHY ``AssetSelection.all()`` (not a hand-listed selection)
----------------------------------------------------------
The 18-asset graph is already ONE connected DAG: ``assets.py`` publishes
``raw/raw_{customers,products,orders,payments}`` and ``dbt_assets.py``'s
``_MedallionDbtTranslator`` resolves every dbt ``source('raw', 'raw_*')`` to
those SAME asset keys, so bronze -> silver -> gold sit downstream of raw by real
asset-dependency edges. ``AssetSelection.all()`` captures every node and Dagster
topologically orders execution from those edges — ingest first, dbt second.

SINGLE-WRITER (the LOCKED requirement)
--------------------------------------
DuckDB is single-writer at the file level. Two guarantees keep this run safe:

  1. Topology serializes the raw/dbt seam — dbt cannot start until its four raw
     upstreams complete, so the two writers never overlap across that edge.
  2. ``dbt build`` is ONE Dagster step with profile ``threads: 1`` — no parallel
     writers inside dbt.

The only residual is the four raw assets fanning out concurrently onto the same
``.duckdb`` file (distinct tables, but one read-write connection at the file
level). Per the architect's recommendation and the LOCKED single-writer rule, we
bake run-level ``max_concurrent=1`` into the job config so the ENTIRE run is
serial end to end. At this data volume the cost is negligible and it removes the
"database is locked" class of failure by construction.

This module is intentionally tiny: it defines ONE job object that
``definitions.py``, ``schedules.py`` and ``sensors.py`` all import, so the
schedule and the failure sensor bind to the SAME instance.
"""

from __future__ import annotations

from dagster import AssetSelection, define_asset_job

# The job's UI name and the stable identifier the schedule + sensor reference.
BACKBONE_JOB_NAME = "backbone_end_to_end"

# Run-level config that serializes every op/asset in the run to ONE at a time.
# This honors the LOCKED DuckDB single-writer requirement: the four raw assets
# (which would otherwise fan out under AssetSelection.all()) run serially, so no
# two steps ever hold a read-write connection on the shared .duckdb file at once.
# multiprocess is the default executor; max_concurrent=1 forces serial execution.
_SERIAL_RUN_CONFIG = {
    "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
}

# The end-to-end job: ingest raw (Postgres -> DuckDB raw.raw_*) THEN dbt build
# (bronze -> silver/*_rejects -> gold), serialized by both topology and config.
backbone_end_to_end = define_asset_job(
    name=BACKBONE_JOB_NAME,
    selection=AssetSelection.all(),
    config=_SERIAL_RUN_CONFIG,
    description=(
        "End-to-end analytical backbone: C2 raw ingestion (Postgres -> DuckDB "
        "raw.raw_*) then the C3 dbt medallion (bronze -> silver/*_rejects -> "
        "gold). Runs serially (max_concurrent=1) to honor the DuckDB "
        "single-writer contract."
    ),
)
