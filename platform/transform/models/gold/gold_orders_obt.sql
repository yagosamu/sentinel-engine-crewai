{{ config(materialized='table') }}

-- gold_orders_obt — one row per ACCEPTED order, denormalized (order + customer
-- attrs + product attrs + payment rollup). Grain: order_id. Read-hot table for
-- AC-2 (p95 <= 5s). Built over silver-ACCEPTED rows ONLY — defects already
-- quarantined upstream, so gold is clean by construction.
--
-- Payment rollup: an order may have 0..n accepted payments. We collapse to one
-- representative payment per order (latest paid_at) so the OBT stays at order
-- grain. payment_* are null for orders with no accepted payment.
with payments_ranked as (
    select
        order_id,
        method as payment_method,
        status as payment_status,
        amount as payment_amount,
        paid_at,
        row_number() over (partition by order_id order by paid_at desc, payment_id desc) as rn
    from {{ ref('silver_payments') }}
),

payment_rollup as (
    select
        order_id,
        payment_method,
        payment_status,
        payment_amount,
        paid_at
    from payments_ranked
    where rn = 1
)

select
    o.order_id,
    o.customer_id,
    c.country as customer_country,
    c.city as customer_city,
    c.segment as customer_segment,
    o.product_id,
    p.sku as product_sku,
    p.name as product_name,
    p.category as product_category,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.ordered_at,
    pr.payment_method,
    pr.payment_status,
    pr.payment_amount,
    pr.paid_at,
    o.is_late
from {{ ref('silver_orders') }} o
left join {{ ref('silver_customers') }} c on c.customer_id = o.customer_id
left join {{ ref('silver_products') }} p on p.product_id = o.product_id
left join payment_rollup pr on pr.order_id = o.order_id
