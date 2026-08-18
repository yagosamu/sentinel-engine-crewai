"""A3 Data Profiler tools — the I3 DuckDB evidence surface (READ-ONLY).

U3 IS RESOLVED BY THE MEDALLION (confirmed against
``platform/transform/macros/classify.sql`` + the silver models): the backbone is
QUARANTINE-NOT-DROP. The classifier stamps ``reject_rule == failure_key`` and the
defect lands in ``silver.silver_<entity>_rejects`` instead of vanishing. So A3
has a *concrete, verifiable* target per failure rather than statistical guessing.

Detection contract (I3), per failure_key:
  negative_price     silver.silver_orders_rejects   WHERE reject_rule = key
  missing_customer   silver.silver_orders_rejects   WHERE reject_rule = key
  invalid_quantity   silver.silver_orders_rejects   WHERE reject_rule = key
  duplicate_order    silver.silver_orders_rejects   WHERE reject_rule = key
  malformed_data     silver.silver_orders_rejects   WHERE reject_rule = key
  destructive_fix    silver.silver_orders_rejects   WHERE reject_rule = key
  orphan_payment     silver.silver_payments_rejects WHERE reject_rule = key
  late_arrival       silver.silver_orders           WHERE is_late          (accepted-flagged)
  schema_drift       silver.silver_orders           WHERE _schema_drift    (accepted-flagged; A2 is primary)
  volume_spike       silver.silver_orders           count vs baseline      (accepted, statistical)

All reads route through ``platform.warehouse.connection.connect_read_only``
(``access_mode=READ_ONLY``) — the tool physically cannot write (R3). SQL uses
bound parameters; table/column identifiers come only from the internal map below,
never from agent input (R8). DuckDB is single-writer: the *harness* kills lock
holders before a run; the tool just opens read-only and serializes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from platform.warehouse.connection import connect_read_only
from typing import Any

from crewai.tools import BaseTool
from pydantic import BaseModel, Field


@dataclass(frozen=True, slots=True)
class _RejectProbe:
    """A reject-table probe: defect lands in ``schema.table`` under reject_rule."""

    schema_table: str
    surface: str  # the human/evidence name (== schema_table without schema prefix)


@dataclass(frozen=True, slots=True)
class _FlagProbe:
    """A flag-column probe: defect is accepted-and-flagged in ``schema.table``."""

    schema_table: str
    flag_sql: str  # a boolean SQL predicate over the accepted table
    surface: str


# Volume spike is statistical: there is no reject row and no boolean flag — the
# signal is a count anomaly. Handled as its own probe kind.
@dataclass(frozen=True, slots=True)
class _VolumeProbe:
    schema_table: str
    surface: str


# The single source of truth mapping failure_key -> its I3 detection probe. Every
# identifier here is an internal constant (never agent-supplied) so it is safe to
# interpolate into the schema-qualified table name; the only *value* (reject_rule)
# is always bound as a parameter.
DETECTION_MAP: dict[str, _RejectProbe | _FlagProbe | _VolumeProbe] = {
    "negative_price": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "missing_customer": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "invalid_quantity": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "duplicate_order": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "malformed_data": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "destructive_fix": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "recurring_incident": _RejectProbe("silver.silver_orders_rejects", "silver_orders_rejects"),
    "orphan_payment": _RejectProbe("silver.silver_payments_rejects", "silver_payments_rejects"),
    "late_arrival": _FlagProbe("silver.silver_orders", "is_late", "silver_orders.is_late"),
    "schema_drift": _FlagProbe("silver.silver_orders", "_schema_drift", "silver_orders._schema_drift"),
    "volume_spike": _VolumeProbe("silver.silver_orders", "silver_orders.count"),
}

# DELIBERATELY ABSENT from DETECTION_MAP (so 11 keys here, not 14) — by design,
# not omission. A maintainer auditing "do all 14 failures have an I3 probe?" should
# find these three handled elsewhere:
#   slow_source            — A2/I1 log-surface failure: detected in Dagster logs,
#                            not the warehouse. Its evidence surface ('dagster_logs')
#                            lives in scoring.EXPECTED_SURFACE, not here. ProfileRejects
#                            returns "no I3 detection probe" for it on purpose.
#   ambiguous_anomaly      — Knowledge/RAG-only: resolved by the manager reasoning over
#                            knowledge/runbook.md, with NO deterministic rule. It has no
#                            DETECTION_MAP probe AND no EXPECTED_SURFACE entry (a correct
#                            ambiguous_anomaly diagnosis scores evidence=0.0). The crew may
#                            QueryDuckDB-count the allow-listed silver tables, but there is
#                            no canonical detection query to register here.
#   multi_failure_cascade  — Flow-only: handled by the deterministic SentinelFlow
#                            (flow.py) and its own cascade scoring tier, not a single-key
#                            probe.
# schema_drift IS present (as a _FlagProbe) even though A2 is its PRIMARY detector:
# the accepted-flag column gives A3 a corroborating I3 signal.

# recurring_incident injects negative_price rows; the rejects table stamps
# 'negative_price'. The profiler queries that reject_rule when probing recurring.
REJECT_RULE_OVERRIDE: dict[str, str] = {"recurring_incident": "negative_price"}

# Allow-listed schema-qualified tables the read-only QueryDuckDB tool may count.
ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        "silver.silver_orders",
        "silver.silver_orders_rejects",
        "silver.silver_payments",
        "silver.silver_payments_rejects",
        "silver.silver_customers",
        "silver.silver_customers_rejects",
        "silver.silver_products",
        "silver.silver_products_rejects",
        "gold.gold_orders_obt",
        "gold.gold_revenue_daily",
    }
)


def _expected_reject_rule(failure_key: str) -> str:
    return REJECT_RULE_OVERRIDE.get(failure_key, failure_key)


class ProfileRejectsArgs(BaseModel):
    failure_key: str = Field(
        ...,
        description="The generator failure_key to probe for in the warehouse (I3).",
    )
    sample_limit: int = Field(
        default=5, ge=0, le=50, description="Max sample rows to return as evidence."
    )


class ProfileRejects(BaseTool):
    """I3: run the canonical detection query for one failure_key, read-only."""

    name: str = "profile_rejects"
    description: str = (
        "Profile the DuckDB warehouse (I3) for a specific generator failure_key. "
        "Returns {found, count, evidence_surface, sample_rows}. Routes each "
        "data-quality failure to its quarantine surface (silver_*_rejects) or "
        "accepted-flag (is_late/_schema_drift) or volume-count signal. Read-only."
    )
    args_schema: type[BaseModel] = ProfileRejectsArgs

    def _run(self, failure_key: str, sample_limit: int = 5) -> str:
        probe = DETECTION_MAP.get(failure_key)
        if probe is None:
            return json.dumps(
                {
                    "found": False,
                    "failure_key": failure_key,
                    "detail": f"no I3 detection probe for failure_key '{failure_key}' "
                    "(it may be A2-only or not data-visible)",
                }
            )
        conn = connect_read_only()
        try:
            if isinstance(probe, _RejectProbe):
                return self._profile_reject(conn, failure_key, probe, sample_limit)
            if isinstance(probe, _FlagProbe):
                return self._profile_flag(conn, failure_key, probe, sample_limit)
            return self._profile_volume(conn, failure_key, probe)
        finally:
            conn.close()

    def _profile_reject(
        self, conn: Any, failure_key: str, probe: _RejectProbe, sample_limit: int
    ) -> str:
        rule = _expected_reject_rule(failure_key)
        count = conn.execute(
            f"SELECT count(*) FROM {probe.schema_table} WHERE reject_rule = ?",  # noqa: S608 - identifier is an internal constant
            [rule],
        ).fetchone()[0]
        samples: list[dict[str, Any]] = []
        if count and sample_limit:
            cur = conn.execute(
                f"SELECT * FROM {probe.schema_table} WHERE reject_rule = ? LIMIT ?",  # noqa: S608
                [rule, sample_limit],
            )
            cols = [d[0] for d in cur.description]
            samples = [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        return json.dumps(
            {
                "found": count > 0,
                "failure_key": failure_key,
                "reject_rule": rule,
                "count": int(count),
                "evidence_surface": probe.surface,
                "sample_rows": samples,
            },
            default=str,
        )

    def _profile_flag(
        self, conn: Any, failure_key: str, probe: _FlagProbe, sample_limit: int
    ) -> str:
        count = conn.execute(
            f"SELECT count(*) FROM {probe.schema_table} WHERE {probe.flag_sql}"  # noqa: S608 - flag_sql is an internal constant
        ).fetchone()[0]
        samples: list[dict[str, Any]] = []
        if count and sample_limit:
            cur = conn.execute(
                f"SELECT * FROM {probe.schema_table} WHERE {probe.flag_sql} LIMIT ?",  # noqa: S608
                [sample_limit],
            )
            cols = [d[0] for d in cur.description]
            samples = [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        return json.dumps(
            {
                "found": count > 0,
                "failure_key": failure_key,
                "count": int(count),
                "evidence_surface": probe.surface,
                "sample_rows": samples,
            },
            default=str,
        )

    def _profile_volume(self, conn: Any, failure_key: str, probe: _VolumeProbe) -> str:
        # volume_spike is statistical, NOT a quarantined row: the generator
        # appends a same-minute burst of valid orders. We report the per-minute
        # signal (recent peak vs the typical minute) so the agent can judge it.
        #
        # CAVEAT (detection-seam honesty, U3-adjacent): the deterministic seeder
        # bulk-loads its entire baseline within a single minute, so the all-time
        # max per-minute is a seed artifact (~99k), not a spike. Comparing the
        # all-time peak to the mean therefore over-fires. We instead measure the
        # MOST RECENT minute against the median minute, and we surface the raw
        # numbers rather than asserting a brittle boolean — a 500-row injected
        # burst is only separable on a freshly reseeded baseline. The agent
        # (and any scorer) must weigh this, not trust a magic flag.
        total = conn.execute(f"SELECT count(*) FROM {probe.schema_table}").fetchone()[0]  # noqa: S608
        recent = conn.execute(
            f"""
            WITH per_minute AS (
                SELECT date_trunc('minute', ordered_at) AS m, count(*) AS c
                FROM {probe.schema_table}
                GROUP BY 1
            ),
            ranked AS (
                SELECT m, c, row_number() OVER (ORDER BY m DESC) AS rn FROM per_minute
            )
            SELECT
                coalesce((SELECT c FROM ranked WHERE rn = 1), 0)        AS latest_minute,
                coalesce((SELECT median(c) FROM per_minute), 0)         AS median_minute,
                coalesce((SELECT count(*) FROM per_minute), 0)          AS minutes
            """  # noqa: S608 - schema_table is an internal constant
        ).fetchone()
        latest, median_per_min, minutes = int(recent[0]), float(recent[1] or 0), int(recent[2])
        # A spike is the latest minute standing well above the typical minute.
        spike = latest > max(median_per_min * 10, 200)
        return json.dumps(
            {
                "found": spike,
                "failure_key": failure_key,
                "count": latest,
                "evidence_surface": probe.surface,
                "detail": {
                    "total_rows": int(total),
                    "latest_minute_rows": latest,
                    "median_minute_rows": round(median_per_min, 2),
                    "minutes_observed": minutes,
                    "spike_heuristic": "latest_minute > max(10*median, 200)",
                    "caveat": "seeder bulk-loads baseline in one minute; reseed for a clean spike signal",
                },
            }
        )


class QueryDuckDBArgs(BaseModel):
    table: str = Field(
        ...,
        description="A schema-qualified allow-listed warehouse table to count, e.g. 'silver.silver_orders'.",
    )


class QueryDuckDB(BaseTool):
    """I3: read-only row count over an allow-listed warehouse table.

    Intentionally NOT a raw-SQL tool. The agent picks an allow-listed table; the
    tool runs a fixed, parameter-free COUNT against it over a READ_ONLY
    connection. This mirrors the C5 query-engine discipline (no interpolation of
    agent input into SQL) while still letting A3 corroborate findings.
    """

    name: str = "query_duckdb"
    description: str = (
        "Count rows in an allow-listed DuckDB warehouse table (I3), read-only. "
        "Allowed tables: silver.silver_orders, silver.silver_orders_rejects, "
        "silver.silver_payments, silver.silver_payments_rejects, gold.gold_orders_obt, "
        "gold.gold_revenue_daily (and the customers/products silver pair)."
    )
    args_schema: type[BaseModel] = QueryDuckDBArgs

    def _run(self, table: str) -> str:
        if table not in ALLOWED_TABLES:
            return json.dumps(
                {
                    "ok": False,
                    "detail": f"table '{table}' is not allow-listed",
                    "allowed": sorted(ALLOWED_TABLES),
                }
            )
        conn = connect_read_only()
        try:
            count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # noqa: S608 - table is allow-listed constant
        finally:
            conn.close()
        return json.dumps({"ok": True, "table": table, "count": int(count)})
