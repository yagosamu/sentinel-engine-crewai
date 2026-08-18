{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='product_id',
        pre_hook="{{ evict_reprocessed_window('product_id', 'bronze_products') }}",
    )
}}

-- silver_products — cleaned product dimension (ACCEPTED rows only).
-- Rejects: malformed/null required fields or negative money.
with classified as (
    {{ classify_products() }}
)

select
    product_id,
    sku,
    name,
    category,
    unit_price,
    cost,
    created_at,
    _ingested_at
from classified
where reject_rule is null

{% if is_incremental() %}
    -- Append re-extracted/new ACCEPTED products only; the pre_hook first evicts
    -- any product_id re-ingested with a newer _ingested_at (e.g. ambiguous_anomaly's
    -- price cut). Keyed on PROCESSING time, not created_at. See
    -- incremental_quarantine.sql.
    {{ append_only_new('product_id') }}
{% endif %}
