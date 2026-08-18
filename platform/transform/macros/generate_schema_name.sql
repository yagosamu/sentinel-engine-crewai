{#
  Map a model's configured +schema (bronze / silver / gold) to that LITERAL
  DuckDB schema, instead of dbt's default <target_schema>_<custom_schema>.

  This is the MCP-confirmed dbt-duckdb idiom for the locked namespacing:
  DuckDB schemas raw/bronze/silver/gold as PRIMARY, with the layer prefix ALSO
  retained in every table name (e.g. gold.gold_orders_obt). C2 owns `raw`;
  C3 owns bronze/silver/gold.

  - When a model sets +schema (all of ours do), use it verbatim.
  - When it does not, fall back to the profile's target schema (default 'main').
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
