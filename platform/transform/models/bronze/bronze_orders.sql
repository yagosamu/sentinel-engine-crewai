-- bronze_orders — thin 1:1 view over raw.raw_orders. Zero cleaning.
-- customer_id is ALREADY normalized by C2 (schema_drift owned upstream); bronze
-- does NOT re-resolve drift. _schema_drift is passed through so the signal
-- survives into a dbt-owned layer. No tests on bronze (defects intentional).
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
    _source_watermark,
    _schema_drift
from {{ source('raw', 'raw_orders') }}
