{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='payment_id',
        pre_hook="{{ evict_reprocessed_window('payment_id', 'bronze_payments') }}",
    )
}}

-- silver_payments — ACCEPTED payments only (order_id resolves to a silver_orders
-- row, valid amount/status). Flows to gold. orphan_payment (incl. order_id
-- 999999999) is quarantined into silver_payments_rejects.
with classified as (
    {{ classify_payments() }}
)

select
    payment_id,
    order_id,
    method,
    amount,
    status,
    paid_at,
    _ingested_at
from classified
where reject_rule is null

{% if is_incremental() %}
    -- Append re-extracted/new ACCEPTED payments only; the evict_reprocessed_window
    -- pre_hook first removes any payment_id whose bronze copy was re-ingested with
    -- a newer _ingested_at (so an accepted->orphan transition leaves gold). Keyed
    -- on PROCESSING time (_ingested_at), not paid_at. See incremental_quarantine.sql.
    {{ append_only_new('payment_id') }}
{% endif %}
