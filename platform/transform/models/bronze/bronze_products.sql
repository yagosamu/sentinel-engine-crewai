-- bronze_products — thin 1:1 view over raw.raw_products. Zero cleaning.
select
    product_id,
    sku,
    name,
    category,
    unit_price,
    cost,
    created_at,
    _ingested_at,
    _source_watermark
from {{ source('raw', 'raw_products') }}
