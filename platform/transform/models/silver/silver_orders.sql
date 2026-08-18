{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='order_id',
        pre_hook="{{ evict_reprocessed_window('order_id', 'bronze_orders') }}",
    )
}}

-- silver_orders — ACCEPTED orders only (valid money, quantity > 0, non-null
-- customer_id, valid status, deduped). Flows to gold.
--
-- ACCEPTED-AND-FLAGGED (not rejected, per the failure map):
--   is_late: late_arrival's 45-day backdated row. Flagged via the gap between
--            ordered_at and the ingest wall-clock (_ingested_at) — anomalous but
--            valid, so rejecting it would be a bug.
--   volume_spike / ambiguous_anomaly: valid rows, no flag needed (count/state
--            signals handled downstream, not row defects).
--
-- INCREMENTAL DESIGN (the U3 "caught, not dropped" guarantee on the DEFAULT,
-- non-full-refresh path):
--   * Watermark = _ingested_at (PROCESSING time), NOT ordered_at (event time).
--     C2 stamps a FRESH _ingested_at on EVERY raw upsert — a brand-new PK AND an
--     in-place overwrite of an existing PK — so any row re-extracted this cycle
--     re-enters the window and is re-classified.
--   * The evict_reprocessed_window pre-hook DELETES from this table EVERY
--     order_id whose bronze row moved into the new _ingested_at window (accepted
--     OR rejected), evaluated BEFORE the insert. Then this model APPENDS only the
--     accepted rows from that window. One mechanism handles all transitions:
--       - new row            -> not in table, appended;
--       - accepted->accepted -> evicted then re-appended (idempotent update);
--       - accepted->REJECTED  -> evicted and NOT re-appended (the stale clean row
--         leaves gold; the corruption lands in silver_orders_rejects instead).
--     This is exactly the destructive_fix/malformed_data case a plain
--     delete+insert (which only deletes keys still present in the accepted SELECT)
--     would silently leave stale in gold.
with classified as (
    {{ classify_orders() }}
)

select
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    status,
    ordered_at,
    _ingested_at,
    _schema_drift,
    -- is_late: ordered_at materially predates the moment it was ingested.
    (date_diff('day', ordered_at, _ingested_at) > 7) as is_late
from classified
where reject_rule is null

{% if is_incremental() %}
    {{ append_only_new('order_id') }}
{% endif %}
