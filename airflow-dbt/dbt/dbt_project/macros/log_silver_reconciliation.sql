{#-
    Generic bronze -> silver row-count reconciliation logger.
    Logs one row per _batch_id into governance.dq_audit_log, same pattern
    as the ingestion-layer reconciliation check (06_data_quality.md Problem 8).

    Non-fatal by design: severity is PASS/WARNING only, never raises.
    A delta exceeding threshold_pct is expected for dedup/collapse models
    (account, customer, trade, trade_history) -- that's not a bug, it's
    the model doing its job. Threshold exists to catch delta that's
    ABNORMALLY large, not to enforce 1:1.

    Args:
      model_name      - string, silver model name, used as source_file label
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
    {% set query %}
      with bronze_counts as (
        {{ bronze_count_sql }}
      ),
      silver_counts as (
        select _batch_id, count(*) as silver_cnt
        from {{ silver_relation }}
        group by 1
      ),
      joined as (
        select
          coalesce(b._batch_id, s._batch_id) as batch_id,
          coalesce(b.bronze_cnt, 0)          as bronze_cnt,
          coalesce(s.silver_cnt, 0)          as silver_cnt,
          case
            when coalesce(b.bronze_cnt, 0) = 0 then 0
            else abs(coalesce(b.bronze_cnt, 0) - coalesce(s.silver_cnt, 0))
                 / b.bronze_cnt * 100
          end as delta_pct
        from bronze_counts b
        full outer join silver_counts s
          on b._batch_id = s._batch_id
      )
      insert into governance.dq_audit_log
        (batch_id, check_type, source_file, expected_value, actual_value, severity, message)
      select
        batch_id,
        'silver_reconciliation',
        '{{ model_name }}',
        bronze_cnt,
        silver_cnt,
        case when delta_pct > {{ threshold }} then 'WARNING' else 'PASS' end,
        'bronze=' || bronze_cnt || ' silver=' || silver_cnt ||
        ' delta=' || round(delta_pct, 2) || '% (threshold ' || {{ threshold }} || '%)'
      from joined
    {% endset %}
    {% do run_query(query) %}
    {% do log('logged silver reconciliation: ' ~ model_name, info=true) %}
  {% endif %}
{% endmacro %}