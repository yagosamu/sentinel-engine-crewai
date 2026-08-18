{{ config(materialized='table') }}

-- silver_orders_rejects — the primary quarantine surface for order-level
-- defects. reject_rule == failure_key for:
--   negative_price, invalid_quantity, missing_customer, duplicate_order,
--   malformed_data, destructive_fix (and recurring_incident -> negative_price).
-- Evals assert WHERE reject_rule = '<failure_key>' caught the expected rows AND
-- that gold contains none of them.
{{ build_rejects(classify_orders()) }}
