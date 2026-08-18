{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='customer_id',
        pre_hook="{{ evict_reprocessed_window('customer_id', 'bronze_customers') }}",
    )
}}

-- silver_customers — cleaned customer dimension (ACCEPTED rows only).
-- A row is rejected when a required field is null/blank (malformed dim). The
-- mirror table silver_customers_rejects quarantines exactly those rows; the
-- two together always equal bronze_customers (the eval invariant).
with classified as (
    {{ classify_customers() }}
)

select
    customer_id,
    full_name,
    email,
    country,
    city,
    segment,
    created_at,
    _ingested_at
from classified
where reject_rule is null

{% if is_incremental() %}
    -- Append re-extracted/new ACCEPTED customers only; the pre_hook first evicts
    -- any customer_id re-ingested with a newer _ingested_at (so a clean->malformed
    -- transition leaves the dimension). Keyed on PROCESSING time, not created_at.
    -- See incremental_quarantine.sql.
    {{ append_only_new('customer_id') }}
{% endif %}
