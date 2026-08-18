{#
  INCREMENTAL QUARANTINE SUPPORT
  ==============================
  An accepted silver model (silver_orders / silver_payments / dimensions) only
  emits rows that PASS classification. On the incremental (non-full-refresh) path
  a row can transition accepted -> REJECTED in place (destructive_fix and
  malformed_data UPDATE existing rows so they fail classification). dbt's stock
  delete+insert deletes from the destination only the keys STILL present in the
  model's SELECT output, so a now-rejected key — absent from the accepted SELECT
  — is never deleted and its STALE CLEAN row survives in the accepted table (and
  thus in gold). That is the exact U3 defect-leak the incremental path must not
  have.

  The fix uses the PROCESSING-time watermark _ingested_at, which C2 re-stamps on
  EVERY raw upsert (a brand-new PK and an in-place overwrite alike), so any
  re-extracted row carries a strictly newer _ingested_at than the copy already in
  silver. Two cooperating pieces, both materialization-strategy = append:

    1. evict_reprocessed_window(unique_key, bronze_model) — a pre_hook that
       DELETEs from the accepted table every unique_key for which the bronze
       source now holds a row with a _ingested_at NEWER than the copy currently in
       the accepted table (accepted OR now-rejected — the pre_hook does not care
       about classification, only that the row was re-extracted). This is a
       per-key comparison (no global scalar watermark), so a concurrent delete can
       never lower a boundary the body later relies on — there is no shared
       boundary. Brand-new keys are simply absent and need no delete.

    2. The model body APPENDs only the ACCEPTED bronze rows that are not already
       present in the accepted table on the exact (unique_key, _ingested_at) pair.
       After the pre_hook has evicted every re-extracted key, an accepted
       re-extracted row is absent and gets appended; an unchanged accepted row is
       still present (same pair) and is skipped; a now-rejected re-extracted row
       was evicted and is NOT re-appended (it is quarantined in *_rejects). All
       transitions are handled with no duplicates and no stale rows.

  On the first incremental run (table absent) the pre_hook is a no-op and the
  model does a full build; on --full-refresh dbt rebuilds from scratch and the
  pre_hook is a safe no-op. The not-already-present anti-join lives in each
  model body via {{ already_present_antijoin(...) }}.
#}


{#-
  Pre-hook: evict every unique_key for which bronze now carries a strictly newer
  _ingested_at than the copy in the accepted table (i.e. it was re-extracted).
  No-op unless this is an incremental run against an existing relation.
-#}
{% macro evict_reprocessed_window(unique_key, bronze_model) %}
    {%- if execute and is_incremental() -%}
        delete from {{ this }} as dest
        where exists (
            select 1
            from {{ ref(bronze_model) }} as src
            where src.{{ unique_key }} = dest.{{ unique_key }}
              and src._ingested_at > dest._ingested_at
        )
    {%- else -%}
        {#- no-op on first build / full-refresh -#}
        select 1 where 1 = 0
    {%- endif -%}
{% endmacro %}


{#-
  Body filter (append guard): keep only rows NOT already present in the accepted
  table on the (unique_key, _ingested_at) pair. After the pre_hook eviction this
  admits exactly the re-extracted accepted rows (and brand-new rows), never a
  duplicate of an unchanged row. Emits a leading `and` so it slots into the
  model's WHERE chain under `{% if is_incremental() %}`.
-#}
{% macro append_only_new(unique_key) %}
    and not exists (
        select 1
        from {{ this }} as existing
        where existing.{{ unique_key }} = classified.{{ unique_key }}
          and existing._ingested_at = classified._ingested_at
    )
{% endmacro %}
