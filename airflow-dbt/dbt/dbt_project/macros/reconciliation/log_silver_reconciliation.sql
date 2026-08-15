{#-
    Bronze -> silver row-count reconciliation, per _batch_id.
    Builds the bronze/silver comparison, delegates scoring+insert to
    the shared log_reconciliation core (macros/log_reconciliation.sql).

    A delta exceeding threshold_pct is expected for dedup/collapse
    models (account, customer, trade, trade_history) -- that's not a
    bug, it's the model doing its job. Threshold exists to catch delta
    that's ABNORMALLY large, not to enforce 1:1.

    Args: unchanged from prior version -- run_silver_reconciliation_checks.sql
    calls this the same way as before.
      model_name       - string, silver model name, used as source_file label
      bronze_count_sql - raw SQL string producing (_batch_id, bronze_cnt).
                         Passed as a string (not a relation) so callers can
                         union multiple bronze sources (e.g. account/customer's
                         flat-file + XML dual-source) or apply filters.
      silver_relation  - the silver model relation, e.g. ref('silver_account')
      threshold_pct    - % delta above which severity = WARNING.
                         Defaults to var('reconciliation_threshold_pct', 5)
                         if not passed explicitly per call.
-#}
{% macro log_silver_reconciliation(model_name, bronze_count_sql, silver_relation, threshold_pct=None) %}
  {% if execute %}
    {% set threshold = threshold_pct if threshold_pct is not none else var('reconciliation_threshold_pct', 5) %}
    {% set comparison_sql %}
      with bronze_counts as (
        {{ bronze_count_sql }}
      ),
      silver_counts as (
        select _batch_id, count(*) as silver_cnt
        from {{ silver_relation }}
        group by 1
      )
      select
        coalesce(b._batch_id, s._batch_id) as _batch_id,
        coalesce(b.bronze_cnt, 0)          as expected_cnt,
        coalesce(s.silver_cnt, 0)          as actual_cnt
      from bronze_counts b
      full outer join silver_counts s
        on b._batch_id = s._batch_id
    {% endset %}
    {{ log_reconciliation('silver_reconciliation', model_name, comparison_sql, threshold) }}
  {% endif %}
{% endmacro %}