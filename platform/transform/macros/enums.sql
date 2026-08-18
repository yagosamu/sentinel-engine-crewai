{#
  Single source of truth for the known-good enumerations, mirroring
  src/seed/factories.py (the generator's clean domain). Used by silver accept/
  reject logic and by accepted_values tests so the two never drift apart.

  ORDER_STATUSES  : factories.ORDER_STATUSES
  PAYMENT_STATUSES: factories.PAYMENT_STATUSES
#}
{% macro order_statuses() %}
    {{ return(['placed', 'shipped', 'delivered', 'returned', 'cancelled']) }}
{% endmacro %}

{% macro payment_statuses() %}
    {{ return(['authorized', 'captured', 'refunded', 'failed']) }}
{% endmacro %}

{#- Render a Python-list of strings as a SQL IN (...) tuple of single-quoted literals. -#}
{% macro sql_in_list(values) %}
    ({%- for v in values -%}'{{ v }}'{%- if not loop.last -%}, {% endif -%}{%- endfor -%})
{% endmacro %}
