"""C3 dbt medallion as Dagster assets — the bridge that fuses C2 ingestion and
the dbt bronze/silver/gold/rejects models into ONE connected asset graph.

WHY THIS MODULE EXISTS
----------------------
C2 (``assets.py``) lands ``raw.raw_*`` in the shared DuckDB file. C3 (the dbt
project at ``platform/transform``) reads those exact tables via
``source('raw', 'raw_<entity>')`` and builds bronze -> silver (+ *_rejects) ->
gold in the SAME file. This module turns every dbt node into a Dagster asset so
the UI shows the full lineage as one graph and a single "Materialize all" runs
ingest -> dbt end to end.

THE JOIN SEAM (the whole point)
-------------------------------
dbt's source ``raw.raw_orders`` must resolve to the SAME Dagster asset key the
ingestion asset already publishes: ``AssetKey(["raw", "raw_orders"])``. With the
default :class:`DagsterDbtTranslator`, a dbt source's asset key is
``source_name / table_name`` -> ``raw / raw_orders``, which already matches the
ingestion key EXACTLY (verified against manifest.json). So the graph connects
with zero source-file edits.

We still subclass the translator (:class:`_MedallionDbtTranslator`) to make that
contract EXPLICIT and tamper-evident: ``get_asset_key`` asserts that every dbt
``raw`` source lands on the canonical ``["raw", "<table>"]`` key, so any future
rename of the dbt source (which would silently orphan bronze from ingestion)
fails loud at definition-load time instead of producing a disconnected graph in
the UI. It also stamps dbt assets with stable Dagster group names per layer.

CONCURRENCY CONTRACT (unchanged)
--------------------------------
DuckDB is single-writer. The connected graph preserves the serialized order:
the dbt models depend on the raw assets, so "Materialize all" runs ingest FIRST
(writes ``raw``) and dbt SECOND (writes bronze/silver/gold). dbt's profile uses
``threads: 1``; ``@dbt_assets`` runs the whole dbt build as one Dagster step, so
no parallel DuckDB writers are introduced.

NO ``from __future__ import annotations`` here, for the same reason as
``assets.py``: Dagster introspects the ``context`` parameter's runtime type hint
at decoration time, and stringized annotations break that check for the
``@dbt_assets`` body.
"""

import os
import shutil
import sys
from collections.abc import Mapping
from pathlib import Path
from platform.warehouse.paths import ENV_VAR, warehouse_path_str
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

# --------------------------------------------------------------------------- #
# Paths — the dbt project that C3 owns.                                        #
# --------------------------------------------------------------------------- #

# This module lives at <repo>/platform/ingestion/dbt_assets.py.
_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
DBT_PROJECT_DIR = _REPO_ROOT / "platform" / "transform"
DBT_PROFILES_DIR = DBT_PROJECT_DIR / "profiles"


def _resolve_dbt_executable() -> str:
    """Return a usable dbt executable path.

    DbtCliResource validates that the dbt binary exists; the bare name ``dbt`` is
    not always on PATH (e.g. under ``dagster dev`` spawned without the venv bin on
    PATH). Prefer the dbt that ships in the active venv's bin dir, then any ``dbt``
    on PATH, then fall back to the literal so the error message stays the standard
    dagster-dbt one.

    NB: do NOT ``Path.resolve()`` ``sys.executable`` first — in a venv that
    symlinks python to the base interpreter, resolving would point at the base
    bin (no ``dbt``) instead of the venv bin. Use ``sys.executable``'s own parent,
    and ``sys.prefix`` as a second venv-aware candidate.
    """
    # The repo's own venv, anchored relative to this file (repo_root/.venv/bin/dbt).
    # This is the most reliable candidate: under `dagster dev` the spawned
    # subprocess may run the BASE interpreter (sys.executable/sys.prefix point
    # outside the venv), so the relative anchor is what actually finds dbt.
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        repo_root / ".venv" / "bin" / "dbt",  # repo venv bin (most reliable)
        Path(sys.executable).parent / "dbt",  # active venv bin (unresolved symlink)
        Path(sys.prefix) / "bin" / "dbt",  # venv prefix bin
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    found = shutil.which("dbt")
    return found or "dbt"


# The dbt binary the DbtCliResource runs. Resolved from the active venv so the
# pipeline loads even when `dbt` is not on the bare PATH.
DBT_EXECUTABLE = _resolve_dbt_executable()

# Belt-and-suspenders: ensure the directory holding dbt is on PATH. DbtProject's
# `prepare_if_dev()` (and other internal dagster-dbt code paths) shell out to the
# bare name `dbt`, bypassing DBT_EXECUTABLE. Under `dagster dev` the spawned code
# server may not have the venv bin on PATH, so we prepend it here at import time.
_dbt_bin_dir = str(Path(DBT_EXECUTABLE).parent)
if _dbt_bin_dir not in os.environ.get("PATH", "").split(os.pathsep):
    os.environ["PATH"] = _dbt_bin_dir + os.pathsep + os.environ.get("PATH", "")

# The raw source asset keys C2 publishes (must equal assets.py @asset key=...).
# Used by the translator's invariant check so a drifted dbt source name can't
# silently disconnect bronze from ingestion.
RAW_SCHEMA = "raw"

# The Dagster group every dbt node lands in, keyed by dbt schema (the medallion
# layer). Falls back to the dbt schema name for anything unmapped.
_LAYER_GROUPS = {
    "bronze": "dbt_bronze",
    "silver": "dbt_silver",
    "gold": "dbt_gold",
}


# --------------------------------------------------------------------------- #
# DbtProject — manages the project + manifest.                                 #
# --------------------------------------------------------------------------- #

# DbtProject points dagster-dbt at the dbt project and tells it where profiles
# live. ``prepare_project_cli_args=["parse", "--quiet"]`` is the default and is
# what regenerates target/manifest.json when running under `dagster dev`
# (prepare_if_dev). The manifest is the single source of truth for the asset DAG.
#
# The dbt profile resolves the DuckDB file from env var DUCKDB_DATABASE. We
# ensure it is set to the canonical warehouse path (paths.py) at import time so
# dbt and the ingestion assets always agree on ONE file — the join seam only
# works if both writers point at the same database.
os.environ.setdefault(ENV_VAR, warehouse_path_str())

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)
# Generate/refresh manifest.json when running under `dagster dev`. A no-op (and
# cheap) when a manifest already exists in a deployed context.
dbt_project.prepare_if_dev()


# --------------------------------------------------------------------------- #
# Translator — make the raw-source -> ingestion-key contract explicit.         #
# --------------------------------------------------------------------------- #


class _MedallionDbtTranslator(DagsterDbtTranslator):
    """Maps dbt nodes onto Dagster asset keys + groups for the medallion.

    Asset keys: the DEFAULT translator already maps dbt source ``raw.raw_orders``
    to ``AssetKey(["raw", "raw_orders"])`` (source_name / table_name), which is
    exactly the key the C2 ingestion asset publishes — so bronze attaches
    downstream of ingestion automatically. We override ``get_asset_key`` ONLY to
    assert that contract for ``raw`` sources (fail loud if a dbt source rename
    would orphan the graph) and otherwise defer to the default for models.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        default_key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props.get("resource_type") == "source":
            source_name = dbt_resource_props.get("source_name")
            table_name = dbt_resource_props.get("name")
            if source_name == RAW_SCHEMA:
                # Canonical key the ingestion asset uses: ["raw", "raw_<entity>"].
                expected = AssetKey([RAW_SCHEMA, table_name])
                if default_key != expected:
                    raise ValueError(
                        f"dbt source '{source_name}.{table_name}' resolves to asset key "
                        f"{default_key.path}, but the C2 ingestion asset publishes "
                        f"{expected.path}. The raw landing zone is the join seam between "
                        "ingestion and dbt; these keys MUST match or bronze will appear as "
                        "an orphan root in the graph. Align the dbt source name/table or the "
                        "ingestion @asset key."
                    )
                return expected
        return default_key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        # Group dbt MODELS by their medallion layer (the dbt schema). Sources are
        # owned by ingestion (group "raw_ingest") and are not re-grouped here.
        if dbt_resource_props.get("resource_type") == "source":
            return None
        schema = dbt_resource_props.get("schema")
        if schema in _LAYER_GROUPS:
            return _LAYER_GROUPS[schema]
        return super().get_group_name(dbt_resource_props)


_translator = _MedallionDbtTranslator()


# --------------------------------------------------------------------------- #
# The dbt asset graph — one Dagster step running `dbt build`.                   #
# --------------------------------------------------------------------------- #


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=_translator)
def dbt_medallion_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Materialize the dbt medallion (bronze -> silver/rejects -> gold).

    Runs ``dbt build``, which executes all models in one step. (``build`` would
    also run dbt data tests, but the project currently declares none — there is
    no ``tests/`` dir and no ``schema.yml`` test blocks, so only models run today;
    the ``+store_failures`` config on gold in ``dbt_project.yml`` is inert until
    tests are added. Quarantine, not dbt tests, is how defects are caught — see
    ``platform/transform/README.md``.) Because the bronze models declare
    ``source('raw', ...)`` deps, these assets sit downstream of the C2 raw
    ingestion assets in the graph — "Materialize all" runs ingest first, then this
    dbt build, honoring the single-writer DuckDB contract.
    """
    yield from dbt.cli(["build"], context=context).stream()
