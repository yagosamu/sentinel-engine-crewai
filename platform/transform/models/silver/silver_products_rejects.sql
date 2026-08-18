{{ config(materialized='table') }}

-- silver_products_rejects — quarantine for malformed/negative-money products.
{{ build_rejects(classify_products()) }}
