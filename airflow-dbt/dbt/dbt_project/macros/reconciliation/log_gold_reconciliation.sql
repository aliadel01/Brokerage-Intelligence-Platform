{#-
    Silver -> gold row-count reconciliation. Full-table comparison, not
    per-batch: gold dims/facts recompute current state each run and
    most gold tables don't even carry a batch_id column (dims never
    do), unlike the incremental bronze->silver append world. So one
    row logged per model, batch_id = -1 sentinel (same -1 convention
    as every dim's own Unknown-member row) -- there's no single batch
    a "total rows now" comparison belongs to.

    Gold count excludes the synthetic Unknown member row (<dim>_sk = -1,
    union all'd in every dim per gold.md) since it's not sourced from
    silver -- counting it would bias every dim toward a false "+1"
    delta. Facts don't add an Unknown row of their own (has_unknown_row
    = false for those calls).

    Same non-fatal PASS/WARNING pattern as log_silver_reconciliation --
    delta can be correct (dedup_latest collapse, filtered inner join),
    not a bug.

    Args:
      model_name       - string, gold model name, used as source_file label
      silver_count_sql - raw SQL string producing a single count, e.g.
                         "select count(*) from " ~ ref('silver_hr')
      gold_relation     - the gold model relation, e.g. ref('dim_broker')
      threshold_pct     - % delta above which severity = WARNING.
                          Defaults to var('reconciliation_threshold_pct', 5).
      has_unknown_row   - whether gold_relation carries the Unknown
                          member row to exclude from its count.
                          Default true (every dim does); pass false for facts.
-#}
{% macro log_gold_reconciliation(model_name, silver_count_sql, gold_relation, threshold_pct=None, has_unknown_row=true) %}
  {% if execute %}
    {% set threshold = threshold_pct if threshold_pct is not none else var('reconciliation_threshold_pct', 5) %}
    {% set offset = 1 if has_unknown_row else 0 %}
    {% set comparison_sql %}
      select
        -1 as batch_id,
        ({{ silver_count_sql }})                      as expected_cnt,
        (select count(*) from {{ gold_relation }}) - {{ offset }} as actual_cnt
    {% endset %}
    {{ log_reconciliation('gold_reconciliation', model_name, comparison_sql, threshold) }}
  {% endif %}
{% endmacro %}