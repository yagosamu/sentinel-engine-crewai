{#
  Render a *_rejects quarantine table body from a classification macro call.

  Selects every classified row WHERE reject_rule IS NOT NULL, exposing all the
  original source columns verbatim (so the defect is inspectable) plus the three
  contract columns: reject_reason TEXT, reject_rule TEXT, rejected_at TIMESTAMPTZ.

  Usage in a rejects model:
      {{ build_rejects(classify_orders()) }}

  classification_sql must already SELECT *, reject_rule, reject_reason. We wrap
  it in a CTE, keep the rejected rows, and stamp rejected_at. The original
  reject_rule/reject_reason from the classifier are surfaced as the contract
  columns (they are TEXT; rejected_at is the dbt run wall-clock, UTC).
#}
{% macro build_rejects(classification_sql) %}
    with classified as (
        {{ classification_sql }}
    )

    select
        * exclude (reject_rule, reject_reason),
        reject_reason,
        reject_rule,
        now() at time zone 'UTC' as rejected_at
    from classified
    where reject_rule is not null
{% endmacro %}
