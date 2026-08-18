-- bronze_payments — thin 1:1 view over raw.raw_payments. Zero cleaning.
-- orphan_payment's order_id=999999999 passes through intact.
select
    payment_id,
    order_id,
    method,
    amount,
    status,
    paid_at,
    _ingested_at,
    _source_watermark
from {{ source('raw', 'raw_payments') }}
