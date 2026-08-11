{#-
    Runs bronze->silver reconciliation for every silver model, once per
    dbt build. Called from dbt_project.yml on-run-end (see snippet below).

    Threshold reasoning per model (ASSUMPTION -- no real number was given,
    adjust in dbt_project.yml vars or per-call if these are wrong):
      - Archetype A / quasi-CDC append-only: near 1:1 expected -> tight (1-2%).
      - Prospect (SCD1 snapshot): dupes within a batch collapse -> looser (10%).
      - Account/Customer (SCD2, dual-source union, day-collapse + tracked-col
        filter): large expected delta by design -> generous (50%).
      - Trade (state-tracking, many-events -> 1-row-per-trade_id): delta is
        structural, not really "per batch" meaningful past batch1 -> very
        generous (80%), logged mainly for visibility not alerting.
      - Trade history (unions bronze_trade_history + bronze_trade filtered
        to real transitions, batch1 excluded from bronze_trade side):
        moderate expected delta (30%).
-#}
{% macro run_silver_reconciliation_checks() %}
  {% if execute %}

    {# ---- Archetype A: simple pass-through + defensive dedup ---- #}
    {{ log_silver_reconciliation(
        'silver_hr',
        "select _batch_id, count(*) as bronze_cnt from " ~ source('bronze', 'bronze_hr') ~ " group by 1",
        ref('silver_hr'),
        1
    ) }}

    {# ---- Prospect: SCD1 snapshot, latest batch wins on agency_id ---- #}
    {{ log_silver_reconciliation(
        'silver_prospect',
        "select _batch_id, count(*) as bronze_cnt from " ~ source('bronze', 'bronze_prospect') ~ " group by 1",
        ref('silver_prospect'),
        10
    ) }}

    {# ---- Account: dual bronze source (flat + XML) unioned before SCD2 ---- #}
    {{ log_silver_reconciliation(
        'silver_account',
        "select _batch_id, count(*) as bronze_cnt from (
           select _batch_id from " ~ source('bronze', 'bronze_account') ~ "
           union all
           select _batch_id from " ~ source('bronze', 'bronze_mgmt_account') ~ "
         ) u group by 1",
        ref('silver_account'),
        50
    ) }}

    {# ---- Customer: same dual-source pattern as account ---- #}
    {{ log_silver_reconciliation(
        'silver_customer',
        "select _batch_id, count(*) as bronze_cnt from (
           select _batch_id from " ~ source('bronze', 'bronze_customer') ~ "
           union all
           select _batch_id from " ~ source('bronze', 'bronze_mgmt_customer') ~ "
         ) u group by 1",
        ref('silver_customer'),
        50
    ) }}

    {# ---- Trade: state-tracking, collapses multi-event-per-trade to 1 row ---- #}
    {{ log_silver_reconciliation(
        'silver_trade',
        "select _batch_id, count(*) as bronze_cnt from " ~ source('bronze', 'bronze_trade') ~ " group by 1",
        ref('silver_trade'),
        80
    ) }}

    {# ---- Trade history: bronze_trade_history (batch1) + bronze_trade (batch2/3 real transitions) ---- #}
    {{ log_silver_reconciliation(
        'silver_trade_history',
        "select _batch_id, count(*) as bronze_cnt from (
           select _batch_id from " ~ source('bronze', 'bronze_trade_history') ~ "
           union all
           select _batch_id from " ~ source('bronze', 'bronze_trade') ~ " where _batch_id != 1
         ) u group by 1",
        ref('silver_trade_history'),
        30
    ) }}

    {# ---- Quasi-CDC append-only: near 1:1, only exact-dupe rows dropped ---- #}
    {% for m in ['holding_history', 'watch_history', 'daily_market', 'cash_transaction'] %}
      {{ log_silver_reconciliation(
          'silver_' ~ m,
          "select _batch_id, count(*) as bronze_cnt from " ~ source('bronze', 'bronze_' ~ m) ~ " group by 1",
          ref('silver_' ~ m),
          2
      ) }}
    {% endfor %}

  {% endif %}
{% endmacro %}