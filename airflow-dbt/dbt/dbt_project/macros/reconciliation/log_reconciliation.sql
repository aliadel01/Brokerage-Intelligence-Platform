{#-
    Shared core for reconciliation logging. Silver (bronze vs silver,
    per-batch) and gold (silver vs gold, total) both call this -- same
    delta/severity math, same insert, no duplication.

    Caller builds comparison_sql: a query producing one row per
    comparison unit with exactly these columns:
      batch_id     - int. Real _batch_id for per-batch checks, or -1
                     sentinel for total/full-table checks (same
                     convention as the gold Unknown-member row).
      expected_cnt - the "should be" count (bronze for silver check,
                     silver for gold check).
      actual_cnt   - the "actually is" count (silver for silver check,
                     gold for gold check).

    Args:
      check_type     - string, e.g. 'silver_reconciliation' /
                        'gold_reconciliation'. Stored as-is.
      model_name     - string, model name, used as source_file label.
      comparison_sql - raw SQL string, see column contract above.
      threshold_pct  - % delta above which severity = WARNING.

    Non-fatal by design: severity is PASS/WARNING only, never raises.
    Same reasoning throughout: an expected/actual delta can be correct
    behavior (dedup, collapse, filtered join), not a bug -- a human
    reviews it rather than the build failing automatically.
-#}
{% macro log_reconciliation(check_type, model_name, comparison_sql, threshold_pct) %}
  {% if execute %}
    {% set query %}
      with comparison as (
        {{ comparison_sql }}
      ),
      scored as (
        select
          batch_id,
          expected_cnt,
          actual_cnt,
          case
            when expected_cnt = 0 then 0
            else abs(expected_cnt - actual_cnt) / expected_cnt * 100
          end as delta_pct
        from comparison
      )
      insert into governance.dq_audit_log
        (batch_id, check_type, source_file, expected_value, actual_value, severity, message)
      select
        batch_id,
        '{{ check_type }}',
        '{{ model_name }}',
        expected_cnt,
        actual_cnt,
        case when delta_pct > {{ threshold_pct }} then 'WARNING' else 'PASS' end,
        'expected=' || expected_cnt || ' actual=' || actual_cnt ||
        ' delta=' || round(delta_pct, 2) || '% (threshold ' || {{ threshold_pct }} || '%)'
      from scored
    {% endset %}
    {% do run_query(query) %}
    {% do log('logged ' ~ check_type ~ ': ' ~ model_name, info=true) %}
  {% endif %}
{% endmacro %}