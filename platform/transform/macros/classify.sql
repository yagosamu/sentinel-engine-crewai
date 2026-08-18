{#
  CLASSIFICATION MACROS — the single source of truth for the accept/reject
  decision per entity. Each macro emits a SELECT that returns EVERY bronze row
  exactly once, decorated with reject_rule + reject_reason (NULL == accepted).

  The accepted silver model selects `where reject_rule is null`; the *_rejects
  table selects `where reject_rule is not null`. Because both consume the SAME
  classification, the invariant accepted + rejects == bronze holds BY
  CONSTRUCTION (no row can be both accepted and rejected, none can be dropped).

  reject_rule is the machine key == the generator failure_key, so evals join
  silver_<entity>_rejects.reject_rule -> injected_incidents.failure_key.
#}


{#- CUSTOMERS: reject malformed/null required dimension fields. -#}
{% macro classify_customers() %}
    select
        *,
        case
            when customer_id is null then 'malformed_data'
            when full_name is null or trim(full_name) = '' then 'malformed_data'
            when email is null or trim(email) = '' then 'malformed_data'
            when country is null or trim(country) = '' then 'malformed_data'
            when segment is null or trim(segment) = '' then 'malformed_data'
        end as reject_rule,
        case
            when customer_id is null then 'customer_id is null'
            when full_name is null or trim(full_name) = '' then 'full_name is null/blank'
            when email is null or trim(email) = '' then 'email is null/blank'
            when country is null or trim(country) = '' then 'country is null/blank'
            when segment is null or trim(segment) = '' then 'segment is null/blank'
        end as reject_reason
    from {{ ref('bronze_customers') }}
{% endmacro %}


{#- PRODUCTS: reject malformed/null required fields or negative money. -#}
{% macro classify_products() %}
    select
        *,
        case
            when product_id is null then 'malformed_data'
            when sku is null or trim(sku) = '' then 'malformed_data'
            when category is null or trim(category) = '' then 'malformed_data'
            when unit_price is null or unit_price < 0 then 'negative_price'
            when cost is null or cost < 0 then 'negative_price'
        end as reject_rule,
        case
            when product_id is null then 'product_id is null'
            when sku is null or trim(sku) = '' then 'sku is null/blank'
            when category is null or trim(category) = '' then 'category is null/blank'
            when unit_price is null or unit_price < 0 then 'unit_price < 0 or null'
            when cost is null or cost < 0 then 'cost < 0 or null'
        end as reject_reason
    from {{ ref('bronze_products') }}
{% endmacro %}


{#-
  ORDERS: the busiest classifier. Maps the generator's order-level failures to
  reject_rule == failure_key. Evaluation order matters — the FIRST matching rule
  wins (a row corrupted two ways is attributed to the higher-priority defect),
  but every defect class is detectable on its own injection.

  duplicate_order is detected via a window: keep the FIRST row per business key
  (lowest order_id); later duplicates are rejected with reject_rule
  'duplicate_order'. The genuine PK row is preserved; only the dup copy is
  quarantined.

  Accepted/flagged (NOT rejected, per the failure map):
    late_arrival      -> is_late flag (ordered_at older than the run by > LATE_DAYS)
    volume_spike      -> valid rows, count signal only
    ambiguous_anomaly -> cancellations + price cut are legitimate state
    schema_drift      -> recovered in raw (_schema_drift passes through)
-#}
{% macro classify_orders() %}
    {%- set known_statuses = order_statuses() -%}
    with deduped as (
        select
            *,
            -- business key for duplicate detection: same customer, product, qty,
            -- money, status, and timestamp re-inserted as a new PK row.
            row_number() over (
                partition by
                    customer_id, product_id, quantity, unit_price,
                    total_amount, status, ordered_at
                order by order_id
            ) as _dup_rownum
        from {{ ref('bronze_orders') }}
    )

    select
        * exclude (_dup_rownum),
        case
            -- missing_customer: NULL customer_id (orphaned order)
            when customer_id is null then 'missing_customer'
            -- invalid_quantity: non-positive quantity (-5)
            when quantity is null or quantity <= 0 then 'invalid_quantity'
            -- negative_price: negative unit price/total (-49.99). Also covers
            -- recurring_incident (repeated negative_price rows -> same rule).
            when unit_price < 0 or total_amount < 0 then 'negative_price'
            -- destructive_fix: total zeroed while qty*unit_price would be > 0
            when total_amount = 0 and (quantity * unit_price) > 0 then 'destructive_fix'
            -- malformed_data: status outside the known ORDER_STATUSES domain
            when status is null or status not in {{ sql_in_list(known_statuses) }} then 'malformed_data'
            -- duplicate_order: second+ row on the full business key
            when _dup_rownum > 1 then 'duplicate_order'
        end as reject_rule,
        case
            when customer_id is null then 'customer_id is null (orphaned order)'
            when quantity is null or quantity <= 0
                then 'quantity ' || coalesce(cast(quantity as varchar), 'null') || ' violates quantity > 0'
            when unit_price < 0 or total_amount < 0
                then 'negative money: unit_price=' || cast(unit_price as varchar) || ' total_amount=' || cast(total_amount as varchar)
            when total_amount = 0 and (quantity * unit_price) > 0
                then 'total_amount=0 while quantity*unit_price=' || cast(quantity * unit_price as varchar)
            when status is null or status not in {{ sql_in_list(known_statuses) }}
                then 'status ' || coalesce('''' || status || '''', 'null') || ' not in known ORDER_STATUSES'
            when _dup_rownum > 1 then 'duplicate of an earlier order on the full business key'
        end as reject_reason
    from deduped
{% endmacro %}


{#-
  PAYMENTS: depends on accepted orders for referential integrity.
    orphan_payment   -> order_id has no silver_orders match (incl. 999999999)
    malformed_data   -> status outside known PAYMENT_STATUSES
    negative_price   -> amount < 0 (defensive; generator uses positive amounts)
  A payment whose order was itself rejected is an orphan by construction (its
  order is not in silver_orders), so this also fans out upstream order defects
  into a payment-side orphan — correct: gold must not see it.
-#}
{% macro classify_payments() %}
    {%- set known_statuses = payment_statuses() -%}
    select
        b.*,
        case
            when b.order_id is null
                or not exists (select 1 from {{ ref('silver_orders') }} o where o.order_id = b.order_id)
                then 'orphan_payment'
            when b.amount is null or b.amount < 0 then 'negative_price'
            when b.status is null or b.status not in {{ sql_in_list(known_statuses) }} then 'malformed_data'
        end as reject_rule,
        case
            when b.order_id is null
                or not exists (select 1 from {{ ref('silver_orders') }} o where o.order_id = b.order_id)
                then 'order_id ' || coalesce(cast(b.order_id as varchar), 'null') || ' has no accepted silver_orders match'
            when b.amount is null or b.amount < 0 then 'amount ' || coalesce(cast(b.amount as varchar), 'null') || ' < 0'
            when b.status is null or b.status not in {{ sql_in_list(known_statuses) }}
                then 'status ' || coalesce('''' || b.status || '''', 'null') || ' not in known PAYMENT_STATUSES'
        end as reject_reason
    from {{ ref('bronze_payments') }} b
{% endmacro %}
