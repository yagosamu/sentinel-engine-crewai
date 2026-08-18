{{ config(materialized='table') }}

-- gold_revenue_daily — date x category rollup. AC-2 p95 <= 5s FAST PATH for the
-- C5 revenue_by_period / revenue_by_country intents and generate_report.
-- Grain: (order_date, product_category). Built over the clean OBT.
--
-- Revenue counts only orders that represent realized demand: cancelled/returned
-- orders are excluded from gross_revenue so the daily rollup reflects net sales,
-- but order_count keeps ALL accepted orders for that date/category.
select
    cast(ordered_at as date) as order_date,
    product_category,
    count(*) as order_count,
    sum(quantity) as units_sold,
    sum(
        case when status in ('cancelled', 'returned') then 0 else total_amount end
    ) as gross_revenue,
    cast(
        avg(
            case when status in ('cancelled', 'returned') then 0 else total_amount end
        ) as decimal(12, 2)
    ) as avg_order_value
from {{ ref('gold_orders_obt') }}
group by 1, 2
