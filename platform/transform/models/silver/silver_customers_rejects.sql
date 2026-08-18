{{ config(materialized='table') }}

-- silver_customers_rejects — quarantine for malformed/null customer dims.
-- All source columns + reject_reason / reject_rule / rejected_at. reject_rule
-- joins to injected_incidents.failure_key for evals.
{{ build_rejects(classify_customers()) }}
