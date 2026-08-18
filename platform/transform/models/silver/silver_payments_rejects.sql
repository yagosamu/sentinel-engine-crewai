{{ config(materialized='table') }}

-- silver_payments_rejects — quarantine for orphan_payment (no accepted order),
-- plus defensive negative-amount / malformed-status payment defects.
{{ build_rejects(classify_payments()) }}
