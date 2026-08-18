"""C2 ingestion assets — Postgres -> DuckDB ``raw.raw_*`` (defect-faithful).

One software-defined asset per source table (customers, products, orders,
payments). Each:

  1. Reads its prior high-watermark from the LAST successful materialization's
     metadata (``context.instance.fetch_materializations``). Cold start = full
     load.
  2. Extracts the window from Postgres read-only (timestamp-incremental, CDC
     style). orders/payments additionally use a monotonic-PK completeness arm so
     a 45-day backdated ``late_arrival`` row is caught WITHOUT a 45-day re-scan,
     PLUS a bounded PK-recency refresh arm so in-place UPDATES to recent rows
     (``destructive_fix`` / ``malformed_data`` zero/garble ``ORDER BY order_id
     DESC LIMIT N`` rows without moving ``ordered_at``) are re-extracted and
     re-stamped with a fresh ``_ingested_at``. See ``PK_REFRESH_WINDOW`` for the
     bound and the named precondition.
  3. UPSERTs by source PK into ``raw.raw_<entity>`` (MERGE/ON CONFLICT) so
     lookback re-scans are idempotent. Defects are NEVER dropped or repaired —
     that is silver's job.
  4. Stamps every row with ``_ingested_at`` (UTC wall-clock — the C8/AC-3 anchor,
     non-negotiable) and ``_source_watermark`` (this run's high-watermark).
  5. Returns a :class:`MaterializeResult` whose metadata persists the new
     high-watermark for the next run and surfaces row counts in the UI.

SCHEMA DRIFT (orders only): the source ``orders.customer_id`` may have been
renamed to ``user_id`` (the ``schema_drift`` failure). C2 resolves the live
column at runtime via ``information_schema`` — mirroring
:func:`src.gen.repository.order_customer_column` exactly — lands it in the STABLE
``customer_id`` slot, and sets ``_schema_drift = TRUE`` for rows captured while
the source column was ``user_id``. This is the ONLY place drift is normalized;
dbt/bronze pass the flag through untouched.

Money is ``DECIMAL``; timestamps are ``TIMESTAMPTZ`` (tz-aware UTC) end to end.

NOTE: this module deliberately does NOT use ``from __future__ import annotations``.
Dagster validates the ``context`` parameter's type hint at decoration time by
runtime introspection; stringized (postponed) annotations break that check
(``Cannot annotate context parameter with type AssetExecutionContext``). Keeping
annotations eager is the supported idiom for ``@asset`` functions.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import duckdb
import psycopg
import pyarrow as pa
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from .resources import DuckDBResource, PostgresResource

# --------------------------------------------------------------------------- #
# Constants                                                                    #
# --------------------------------------------------------------------------- #

RAW_SCHEMA = "raw"

# Lookback covers ordinary late UPDATES to recently-extracted rows. The wide
# 45-day backdated INSERT (late_arrival) is caught by the monotonic-PK arm, NOT
# by widening this window — so AC-2/AC-3 are never threatened by a big re-scan.
LOOKBACK = timedelta(days=7)

# PK-recency refresh window (orders/payments only). On every INCREMENTAL run we
# also re-extract rows whose PK is within this many of the prior high-watermark
# PK, REGARDLESS of event time. This is the changed-row arm: in-place UPDATEs to
# recent rows that do NOT move the event-time watermark (destructive_fix and
# malformed_data both mutate `ORDER BY order_id DESC LIMIT N` rows, i.e. the
# highest/most-recent PKs, without touching ordered_at) are otherwise invisible
# to a watermark-CDC extractor. Re-extracting the recent PK band re-stamps a
# fresh _ingested_at on those rows so the silver _ingested_at predicate
# re-classifies them — without a full table re-scan (AC-2/AC-3 stay protected).
#
# PRECONDITION (named per R5): an in-place UPDATE to a row OLDER than both the
# event-time LOOKBACK and the PK-recency band is not detectable by timestamp +
# bounded-PK CDC alone (the source has no `updated_at`/CDC column). The DEFAULT
# ingest path (ephemeral instance => FULL reload, used by the evals and the C8
# probe) re-extracts every row with a fresh _ingested_at and so is unconditional;
# the PK-recency arm makes the PERSISTENT (watermark) path correct for the chaos
# modes the generator actually injects (which always target recent rows).
PK_REFRESH_WINDOW = 1000

# Metadata keys persisted on each MaterializeResult and read back next run.
WM_TS_KEY = "high_watermark_ts"  # ISO-8601 string of the max watermark-column value
WM_ID_KEY = "high_watermark_id"  # max source PK (orders/payments completeness arm)

# A floor timestamp for cold start (no prior materialization). Far enough back to
# precede any seeded data; the lookback subtraction below stays well-defined.
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Per-entity extraction spec                                                   #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class EntitySpec:
    """Static description of how one source table maps into ``raw``."""

    entity: str  # 'customers' | 'products' | 'orders' | 'payments'
    source_table: str  # Postgres table name
    pk: str  # source primary-key column
    watermark_col: str  # timestamp column driving incremental extraction
    # raw_<entity> column names, IN ORDER, that come straight from the source
    # SELECT (i.e. excluding the C2-stamped _ingested_at / _source_watermark /
    # _schema_drift trailing columns).
    source_columns: tuple[str, ...]
    # True for orders/payments: also capture rows with pk > prior_max_pk (the
    # late_arrival completeness backstop) AND rows within PK_REFRESH_WINDOW of the
    # prior high PK (the in-place-update changed-row arm). Both run regardless of
    # the time watermark.
    #
    # PRECONDITION for the dimensions (customers/products, use_pk_arm=False) on
    # the PERSISTENT/incremental path: a dimension row whose created_at predates
    # the LOOKBACK but is INSERTed/UPDATEd after the prior extraction snapshot
    # (clock skew, long transaction, backfill, or an in-place edit that does not
    # move created_at) is NOT re-extracted by the time arm alone and would be
    # missed. The generator injects no such dimension mutation (its dimension
    # defects are present at seed time, captured on the cold full load), and the
    # DEFAULT ingest path is a full reload, so this is sound for the program as
    # scoped — but it is a real assumption, named here rather than left implicit.
    # To remove it, add a processing-time (_ingested_at-style) source column or a
    # dimension PK-recency arm; deferred as out of the committed dimension scope.
    use_pk_arm: bool


CUSTOMERS = EntitySpec(
    entity="customers",
    source_table="customers",
    pk="customer_id",
    watermark_col="created_at",
    source_columns=("customer_id", "full_name", "email", "country", "city", "segment", "created_at"),
    use_pk_arm=False,
)

PRODUCTS = EntitySpec(
    entity="products",
    source_table="products",
    pk="product_id",
    watermark_col="created_at",
    source_columns=("product_id", "sku", "name", "category", "unit_price", "cost", "created_at"),
    use_pk_arm=False,
)

# orders.source_columns lists the STABLE raw slots. The customer column is the
# slot name "customer_id"; the live source column (customer_id|user_id) is
# resolved at runtime and SELECTed AS customer_id, so the tuple is correct
# regardless of drift.
ORDERS = EntitySpec(
    entity="orders",
    source_table="orders",
    pk="order_id",
    watermark_col="ordered_at",
    source_columns=(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "status",
        "ordered_at",
    ),
    use_pk_arm=True,
)

PAYMENTS = EntitySpec(
    entity="payments",
    source_table="payments",
    pk="payment_id",
    watermark_col="paid_at",
    source_columns=("payment_id", "order_id", "method", "amount", "status", "paid_at"),
    use_pk_arm=True,
)


# --------------------------------------------------------------------------- #
# DDL — raw schema + tables (permissive types so defects land intact)         #
# --------------------------------------------------------------------------- #

# TEXT for status (no CHECK), BIGINT pks, DECIMAL money, TIMESTAMPTZ time.
# These tables mirror Postgres 1:1; the trailing underscore-prefixed columns are
# C2-owned lineage stamps.
_DDL: dict[str, str] = {
    "customers": """
        CREATE TABLE IF NOT EXISTS raw.raw_customers (
            customer_id        BIGINT,
            full_name          TEXT,
            email              TEXT,
            country            TEXT,
            city               TEXT,
            segment            TEXT,
            created_at         TIMESTAMPTZ,
            _ingested_at       TIMESTAMPTZ,
            _source_watermark  TIMESTAMPTZ,
            PRIMARY KEY (customer_id)
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS raw.raw_products (
            product_id         BIGINT,
            sku                TEXT,
            name               TEXT,
            category           TEXT,
            unit_price         DECIMAL(10,2),
            cost               DECIMAL(10,2),
            created_at         TIMESTAMPTZ,
            _ingested_at       TIMESTAMPTZ,
            _source_watermark  TIMESTAMPTZ,
            PRIMARY KEY (product_id)
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS raw.raw_orders (
            order_id           BIGINT,
            customer_id        BIGINT,
            product_id         BIGINT,
            quantity           INTEGER,
            unit_price         DECIMAL(10,2),
            total_amount       DECIMAL(12,2),
            status             TEXT,
            ordered_at         TIMESTAMPTZ,
            _ingested_at       TIMESTAMPTZ,
            _source_watermark  TIMESTAMPTZ,
            _schema_drift      BOOLEAN,
            PRIMARY KEY (order_id)
        )
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS raw.raw_payments (
            payment_id         BIGINT,
            order_id           BIGINT,
            method             TEXT,
            amount             DECIMAL(12,2),
            status             TEXT,
            paid_at            TIMESTAMPTZ,
            _ingested_at       TIMESTAMPTZ,
            _source_watermark  TIMESTAMPTZ,
            PRIMARY KEY (payment_id)
        )
    """,
}


def _ensure_table(duck: duckdb.DuckDBPyConnection, entity: str) -> None:
    duck.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    duck.execute(_DDL[entity])


# --------------------------------------------------------------------------- #
# Watermark read (from the prior successful materialization)                   #
# --------------------------------------------------------------------------- #


def _read_prior_watermark(context: AssetExecutionContext, asset_key: AssetKey) -> tuple[datetime, int]:
    """Return (prior_high_ts, prior_high_id) from the last materialization.

    Cold start (no prior materialization, or metadata absent) returns
    ``(_EPOCH, 0)`` => effectively a full load on the first run.
    """
    prior_ts = _EPOCH
    prior_id = 0
    try:
        records = context.instance.fetch_materializations(records_filter=asset_key, limit=1)
    except Exception as exc:  # noqa: BLE001 - instance is best-effort; cold start is the safe fallback
        # Cold-start fallback is SAFE (full reload, idempotent upsert) but
        # EXPENSIVE; log at warning so a genuine instance/storage misconfiguration
        # is not silently masked as a recurring full scan every run.
        context.log.warning(
            "%s: could not read prior watermark from the instance (%s: %s); "
            "falling back to a cold-start FULL load this run.",
            asset_key.to_user_string(),
            type(exc).__name__,
            exc,
        )
        return prior_ts, prior_id
    if not records.records:
        return prior_ts, prior_id
    mat = records.records[0].asset_materialization
    if mat is None:
        return prior_ts, prior_id
    meta = mat.metadata or {}
    ts_entry = meta.get(WM_TS_KEY)
    if ts_entry is not None:
        raw_value = getattr(ts_entry, "value", ts_entry)
        parsed = _parse_ts(raw_value)
        if parsed is not None:
            prior_ts = parsed
    id_entry = meta.get(WM_ID_KEY)
    if id_entry is not None:
        raw_value = getattr(id_entry, "value", id_entry)
        try:
            prior_id = int(raw_value)
        except (TypeError, ValueError):
            prior_id = 0
    return prior_ts, prior_id


def _parse_ts(value: object) -> datetime | None:
    """Coerce a stored watermark value (ISO string or epoch float) to aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        # MetadataValue.timestamp stores epoch seconds.
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


# --------------------------------------------------------------------------- #
# schema_drift resolution (orders only) — mirrors src.gen.repository           #
# --------------------------------------------------------------------------- #


def resolve_order_customer_column(conn: psycopg.Connection) -> str:
    """Return the live source column for the order->customer link.

    EXACT mirror of :func:`src.gen.repository.order_customer_column`: returns
    ``'user_id'`` after the ``schema_drift`` failure renamed the column, else
    ``'customer_id'``. C2 selects this column ``AS customer_id`` into the stable
    raw slot.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'orders' AND column_name IN ('customer_id', 'user_id')"
        )
        row = cur.fetchone()
    return row[0] if row else "customer_id"


# --------------------------------------------------------------------------- #
# Extraction + load                                                            #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class LoadStats:
    """What one entity's extraction produced — surfaced as run metadata."""

    rows_read: int
    rows_upserted: int
    new_high_ts: datetime
    new_high_id: int
    full_load: bool
    schema_drift: bool  # orders: True if source column was user_id this run


def _extract_rows(
    pg: psycopg.Connection,
    spec: EntitySpec,
    prior_ts: datetime,
    prior_id: int,
) -> tuple[list[tuple], bool]:
    """Run the windowed SELECT and return (rows, schema_drift_flag).

    ``schema_drift_flag`` is only meaningful for orders (False otherwise).
    """
    schema_drift = False
    # Build the projection. For orders, resolve the (possibly drifted) customer
    # column and alias it back to the stable slot name.
    if spec.entity == "orders":
        live_col = resolve_order_customer_column(pg)
        schema_drift = live_col != "customer_id"
        projection_cols = []
        for col in spec.source_columns:
            if col == "customer_id":
                projection_cols.append(f"{live_col} AS customer_id")
            else:
                projection_cols.append(col)
        projection = ", ".join(projection_cols)
    else:
        projection = ", ".join(spec.source_columns)

    window_start = prior_ts - LOOKBACK
    # Three extraction arms (orders/payments use all three; dimensions use only
    # the time arm):
    #   1. TIME arm:        rows whose watermark moved inside the lookback window
    #                       (ordinary late/updated rows near the frontier).
    #   2. PK-NEW arm:      rows with a brand-new PK > prior high PK, regardless
    #                       of (possibly backdated) event time => late_arrival.
    #   3. PK-RECENCY arm:  rows whose PK is within PK_REFRESH_WINDOW of the prior
    #                       high PK, regardless of event time => in-place UPDATEs
    #                       to recent rows (destructive_fix / malformed_data) that
    #                       do NOT move the event-time watermark. Re-stamps a
    #                       fresh _ingested_at so silver re-classifies them.
    if spec.use_pk_arm:
        refresh_floor = prior_id - PK_REFRESH_WINDOW
        where = f"({spec.watermark_col} >= %s) OR ({spec.pk} > %s) OR ({spec.pk} >= %s)"
        params: tuple = (window_start, prior_id, refresh_floor)
    else:
        where = f"{spec.watermark_col} >= %s"
        params = (window_start,)

    sql = f"SELECT {projection} FROM {spec.source_table} WHERE {where} ORDER BY {spec.pk}"  # noqa: S608 - identifiers are static spec constants, only values are bound
    with pg.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return rows, schema_drift


def _upsert(
    duck: duckdb.DuckDBPyConnection,
    spec: EntitySpec,
    rows: list[tuple],
    ingested_at: datetime,
    new_high_ts: datetime,
    schema_drift: bool,
) -> int:
    """Idempotently upsert rows into raw.raw_<entity>. Returns rows upserted.

    Uses DuckDB ``INSERT ... SELECT ... ON CONFLICT (pk) DO UPDATE`` so a lookback
    re-scan overwrites the same PK (idempotent) while a genuinely new PK (e.g.
    duplicate_order's real second row) lands as a distinct row. Defects are
    written verbatim — no filtering, no repair.

    BULK, set-based write. The whole batch is assembled into an in-memory Arrow
    table, registered as a virtual relation, and written with ONE vectorized
    statement. The earlier per-row ``executemany`` re-planned and re-executed the
    upsert once *per row* — 800k+ prepared-statement executions, each doing its
    own index conflict-check — which made the orders full load take 16+ minutes at
    pegged CPU. The DuckDB Python docs explicitly warn against ``executemany`` for
    bulk loads and point to registering an Arrow/DataFrame object instead
    (https://duckdb.org/docs/current/clients/python/dbapi#prepared-statements,
    https://duckdb.org/docs/current/clients/python/data_ingestion). Column types
    are guaranteed by the typed target DDL: DuckDB casts on INSERT...SELECT, so
    Arrow type inference (including all-NULL columns) never compromises fidelity.
    """
    if not rows:
        return 0

    target = f"{RAW_SCHEMA}.raw_{spec.entity}"

    # Stamp trailing C2-owned columns onto every row tuple.
    # _source_watermark for orders/payments stores the TS arm value (the time
    # high-watermark); the id arm is persisted separately in run metadata.
    if spec.entity == "orders":
        all_cols = (*spec.source_columns, "_ingested_at", "_source_watermark", "_schema_drift")
        stamped = [(*row, ingested_at, new_high_ts, schema_drift) for row in rows]
    else:
        all_cols = (*spec.source_columns, "_ingested_at", "_source_watermark")
        stamped = [(*row, ingested_at, new_high_ts) for row in rows]

    col_list = ", ".join(all_cols)
    # ON CONFLICT updates every non-PK column to the freshly-extracted values.
    update_cols = [c for c in all_cols if c != spec.pk]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    # Column-orient the stamped rows into an Arrow table (the in-memory relation).
    # Defects (negative prices, NULLs, drift flags) ride through verbatim; the
    # target DDL's types do the casting on INSERT...SELECT.
    columns = list(zip(*stamped, strict=True))
    batch = pa.table({name: pa.array(list(col)) for name, col in zip(all_cols, columns, strict=True)})

    # Register under a per-entity name so concurrent specs never collide, and
    # always unregister so the virtual relation does not outlive this write.
    relation = f"_c2_upsert_{spec.entity}"
    duck.register(relation, batch)
    try:
        sql = (
            f"INSERT INTO {target} ({col_list}) "  # noqa: S608 - identifiers are static spec constants
            f"SELECT {col_list} FROM {relation} "
            f"ON CONFLICT ({spec.pk}) DO UPDATE SET {set_clause}"
        )
        duck.execute(sql)
    finally:
        duck.unregister(relation)
    # rows upserted == rows presented (insert or update both count as written).
    return len(stamped)


def _compute_high_watermark(
    rows: list[tuple],
    spec: EntitySpec,
    prior_ts: datetime,
    prior_id: int,
) -> tuple[datetime, int]:
    """Derive the new (high_ts, high_id) from the extracted rows.

    Never regresses below the prior watermark even if the window was empty.
    """
    ts_index = spec.source_columns.index(spec.watermark_col)
    pk_index = spec.source_columns.index(spec.pk)
    high_ts = prior_ts
    high_id = prior_id
    for row in rows:
        ts_val = row[ts_index]
        if isinstance(ts_val, datetime):
            aware = ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=UTC)
            if aware > high_ts:
                high_ts = aware
        pk_val = row[pk_index]
        if pk_val is not None and int(pk_val) > high_id:
            high_id = int(pk_val)
    return high_ts, high_id


def ingest_entity(
    context: AssetExecutionContext,
    spec: EntitySpec,
    postgres: PostgresResource,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """Shared body for all four raw assets. Extract -> upsert -> metadata."""
    asset_key = context.asset_key
    prior_ts, prior_id = _read_prior_watermark(context, asset_key)
    full_load = prior_ts <= _EPOCH and prior_id == 0
    ingested_at = datetime.now(UTC)

    with postgres.connect() as pg:
        rows, schema_drift = _extract_rows(pg, spec, prior_ts, prior_id)

    new_high_ts, new_high_id = _compute_high_watermark(rows, spec, prior_ts, prior_id)

    with duckdb_resource.connect() as duck:
        _ensure_table(duck, spec.entity)
        rows_upserted = _upsert(
            duck,
            spec,
            rows,
            ingested_at,
            new_high_ts,
            schema_drift,
        )
        total = duck.execute(f"SELECT count(*) FROM {RAW_SCHEMA}.raw_{spec.entity}").fetchone()[0]

    stats = LoadStats(
        rows_read=len(rows),
        rows_upserted=rows_upserted,
        new_high_ts=new_high_ts,
        new_high_id=new_high_id,
        full_load=full_load,
        schema_drift=schema_drift,
    )

    context.log.info(
        "raw_%s: read=%d upserted=%d full_load=%s schema_drift=%s high_ts=%s high_id=%d total=%d",
        spec.entity,
        stats.rows_read,
        stats.rows_upserted,
        stats.full_load,
        stats.schema_drift,
        stats.new_high_ts.isoformat(),
        stats.new_high_id,
        total,
    )

    metadata: dict[str, MetadataValue] = {
        "rows_read": MetadataValue.int(stats.rows_read),
        "rows_upserted": MetadataValue.int(stats.rows_upserted),
        "raw_table_total": MetadataValue.int(int(total)),
        "full_load": MetadataValue.bool(stats.full_load),
        "watermark_col": MetadataValue.text(spec.watermark_col),
        # The persisted high-watermark, read back by the next run.
        WM_TS_KEY: MetadataValue.text(stats.new_high_ts.isoformat()),
        WM_ID_KEY: MetadataValue.int(stats.new_high_id),
        "dagster/row_count": MetadataValue.int(int(total)),
    }
    if spec.entity == "orders":
        metadata["schema_drift"] = MetadataValue.bool(stats.schema_drift)

    return MaterializeResult(metadata=metadata)


# --------------------------------------------------------------------------- #
# The four software-defined assets                                            #
# --------------------------------------------------------------------------- #


@asset(key=["raw", "raw_customers"], group_name="raw_ingest", compute_kind="postgres")
def raw_customers(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """raw.raw_customers — 1:1 mirror of source customers (watermark=created_at)."""
    return ingest_entity(context, CUSTOMERS, postgres, duckdb_resource)


@asset(key=["raw", "raw_products"], group_name="raw_ingest", compute_kind="postgres")
def raw_products(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """raw.raw_products — 1:1 mirror of source products (watermark=created_at)."""
    return ingest_entity(context, PRODUCTS, postgres, duckdb_resource)


@asset(key=["raw", "raw_orders"], group_name="raw_ingest", compute_kind="postgres")
def raw_orders(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """raw.raw_orders — mirror of source orders with schema_drift normalization.

    Resolves the live customer column (customer_id|user_id), lands it in the
    stable customer_id slot, and flags _schema_drift. Dual-watermark:
    ordered_at (time) + order_id (completeness). Defects intact.
    """
    return ingest_entity(context, ORDERS, postgres, duckdb_resource)


@asset(key=["raw", "raw_payments"], group_name="raw_ingest", compute_kind="postgres")
def raw_payments(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """raw.raw_payments — 1:1 mirror with dual-watermark (paid_at + payment_id).

    orphan_payment's order_id=999999999 lands intact (no FK enforced in raw).
    """
    return ingest_entity(context, PAYMENTS, postgres, duckdb_resource)


ALL_ASSETS = [raw_customers, raw_products, raw_orders, raw_payments]
