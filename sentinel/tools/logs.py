"""A2 Log Analyst tools — the I1 (Dagster) and I2 (dbt) evidence surfaces.

Both tools are strictly READ-ONLY (R3): they open log files / artifacts the
backbone produced and parse them. They never write, never start ``dagster dev``,
and never touch the warehouse.

I1 — Dagster run logs (:class:`ReadDagsterLogs`)
    The backbone failure sensor (``platform/ingestion/sensors.py``) emits a
    stable, grep-able line on any failed run::

        BACKBONE_RUN_FAILURE run_id=<id> job=<job> error=<message>

    That ``FAILURE_LOG_PREFIX`` is a documented contract. This tool scans the
    text logs under ``DAGSTER_HOME`` (the structured ``logs/event.log`` and the
    per-run ``storage/<run_id>/compute_logs/*.err|*.out``) for that prefix and
    parses the ``key=value`` fields deterministically. It carries the
    ``slow_source`` reliability story: a stalled source makes log reads flaky,
    so the tool takes a per-call timeout and the A2 agent gets ``max_retry_limit``.

I2 — dbt run results (:class:`ReadDbtRunResults`)
    Loads ``platform/transform/target/run_results.json`` and returns the failed
    / error nodes. This is where ``schema_drift`` surfaces (the renamed
    ``customer_id -> user_id`` breaks downstream refs and a node errors).
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, ClassVar

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

# The contract prefix the backbone sensor emits (mirror of
# platform.ingestion.sensors.FAILURE_LOG_PREFIX — duplicated as a literal so the
# read side does not import Component A internals, only its emitted exhaust).
FAILURE_LOG_PREFIX = "BACKBONE_RUN_FAILURE"


def _dagster_home() -> Path:
    """Resolve DAGSTER_HOME from the env, falling back to the repo default."""
    raw = os.environ.get("DAGSTER_HOME")
    if raw:
        return Path(raw).expanduser().resolve()
    # repo-root/.dagster_home — this file is sentinel/tools/logs.py.
    return (Path(__file__).resolve().parents[2] / ".dagster_home").resolve()


def _dbt_target_dir() -> Path:
    """Default dbt target dir under the read-only platform tree."""
    return (Path(__file__).resolve().parents[2] / "platform" / "transform" / "target").resolve()


def parse_failure_line(line: str) -> dict[str, str] | None:
    """Parse one ``BACKBONE_RUN_FAILURE key=value ...`` line.

    Returns a dict of the parsed fields (``run_id``, ``job``, ``error``, ...) or
    ``None`` if the line does not carry the contract prefix. ``error`` may itself
    contain spaces, so everything after ``error=`` is captured verbatim.
    """
    if FAILURE_LOG_PREFIX not in line:
        return None
    tail = line.split(FAILURE_LOG_PREFIX, 1)[1].strip()
    fields: dict[str, str] = {}
    # error= must be greedy to the end of line; pull it first.
    if "error=" in tail:
        head, err = tail.split("error=", 1)
        fields["error"] = err.strip()
        tail = head.strip()
    for token in tail.split():
        if "=" in token:
            key, _, value = token.partition("=")
            fields[key] = value
    return fields or None


def _iter_log_files(home: Path) -> list[Path]:
    """Text log files under DAGSTER_HOME that may carry the failure line."""
    files: list[Path] = []
    event_log = home / "logs" / "event.log"
    if event_log.is_file():
        files.append(event_log)
    storage = home / "storage"
    if storage.is_dir():
        files.extend(sorted(storage.glob("*/compute_logs/*.err")))
        files.extend(sorted(storage.glob("*/compute_logs/*.out")))
    return files


class ReadDagsterLogsArgs(BaseModel):
    run_id: str = Field(
        default="latest",
        description="A specific Dagster run_id to filter on, or 'latest' for the most recent failure line.",
    )
    timeout_seconds: float = Field(
        default=5.0,
        ge=0.1,
        description="Per-call wall-clock budget; a stalled source makes log reads flaky (slow_source).",
    )


class ReadDagsterLogs(BaseTool):
    """I1: scan DAGSTER_HOME text logs for the BACKBONE_RUN_FAILURE contract line."""

    name: str = "read_dagster_logs"
    description: str = (
        "Read the backbone's Dagster run logs (I1) and return any "
        "BACKBONE_RUN_FAILURE record parsed into {run_id, job, error}. Use this "
        "to detect pipeline-level failures such as schema_drift and slow_source. "
        "Read-only; never starts Dagster."
    )
    args_schema: type[BaseModel] = ReadDagsterLogsArgs

    def _run(self, run_id: str = "latest", timeout_seconds: float = 5.0) -> str:
        home = _dagster_home()
        deadline = time.monotonic() + timeout_seconds
        records: list[dict[str, str]] = []
        for path in _iter_log_files(home):
            if time.monotonic() > deadline:
                return json.dumps(
                    {
                        "found": False,
                        "timed_out": True,
                        "detail": f"log scan exceeded {timeout_seconds}s budget (slow_source signal)",
                        "dagster_home": str(home),
                    }
                )
            try:
                with path.open("r", encoding="utf-8", errors="replace") as handle:
                    for line in handle:
                        parsed = parse_failure_line(line)
                        if parsed is None:
                            continue
                        if run_id != "latest" and parsed.get("run_id") != run_id:
                            continue
                        records.append(parsed)
            except OSError:
                continue
        if not records:
            return json.dumps(
                {
                    "found": False,
                    "detail": "no BACKBONE_RUN_FAILURE line found in Dagster logs",
                    "dagster_home": str(home),
                }
            )
        chosen = records[-1] if run_id == "latest" else records[0]
        return json.dumps({"found": True, "record": chosen, "count": len(records)})


class ReadDbtRunResultsArgs(BaseModel):
    target_dir: str = Field(
        default="",
        description="dbt target directory; empty uses platform/transform/target.",
    )


class ReadDbtRunResults(BaseTool):
    """I2: load dbt run_results.json and return failed/error nodes."""

    name: str = "read_dbt_run_results"
    description: str = (
        "Read the backbone's dbt run results (I2) from run_results.json and "
        "return any nodes whose status is not 'success' as {node, status, "
        "message}. schema_drift surfaces here as a model error. Read-only."
    )
    args_schema: type[BaseModel] = ReadDbtRunResultsArgs

    # statuses dbt uses for a non-passing node
    FAILED_STATUSES: ClassVar[tuple[str, ...]] = ("error", "fail", "runtime error", "skipped")

    def _run(self, target_dir: str = "") -> str:
        base = Path(target_dir).expanduser().resolve() if target_dir else _dbt_target_dir()
        results_file = base / "run_results.json"
        if not results_file.is_file():
            return json.dumps(
                {"found": False, "detail": f"run_results.json not found at {results_file}"}
            )
        try:
            data: dict[str, Any] = json.loads(results_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return json.dumps({"found": False, "detail": f"could not parse run_results.json: {exc}"})

        failures = []
        for result in data.get("results", []):
            status = str(result.get("status", "")).lower()
            if status in self.FAILED_STATUSES:
                failures.append(
                    {
                        "node": result.get("unique_id", "<unknown>"),
                        "status": status,
                        "message": result.get("message") or "",
                    }
                )
        return json.dumps(
            {
                "found": bool(failures),
                "failed_nodes": failures,
                "total_nodes": len(data.get("results", [])),
            }
        )
