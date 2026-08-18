-- bronze_customers — thin 1:1 view over raw.raw_customers. Zero cleaning.
-- Defects (if any) pass through intact; the first place a row can be rejected
-- is silver. No tests on bronze.
select
    customer_id,
    full_name,
    email,
    country,
    city,
    segment,
    created_at,
    _ingested_at,
    _source_watermark
from {{ source('raw', 'raw_customers') }}
