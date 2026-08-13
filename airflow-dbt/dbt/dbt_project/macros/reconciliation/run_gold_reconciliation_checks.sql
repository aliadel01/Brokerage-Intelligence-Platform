{#-
    Runs silver->gold reconciliation for every gold model, once per dbt
    build. Called from dbt_project.yml on-run-end, same wiring pattern
    as run_silver_reconciliation_checks().

    Threshold reasoning per model group (ASSUMPTION, same caveat as
    silver's -- adjust in dbt_project.yml vars or per-call if wrong):
      - Dims: direct pass-through (Archetype A) or SCD2 1-row-per-version
        (dim_customer/dim_account) -> gold is silver + Unknown row,
        near 1:1 -> tight (2%).
      - Dims: dedup_latest collapse, many silver versions -> one gold
        row per key (dim_company by cik, dim_security by symbol) ->
        large expected delta by design -> generous (60%).
      - Facts: direct pass-through, only left joins for FK resolution,
        no row-dropping join -> near 1:1 -> tight (2%).
      - Facts: inner join to fact_trade for derived FKs (ADR-005/010)
        -> orphan rows (trade_id not found) can drop -> moderate (15%).
-#}
{% macro run_gold_reconciliation_checks() %}
  {% if execute %}

    {# ---- Dims: direct pass-through / SCD2 versioned, + Unknown row ---- #}
    {% set direct_dims = [
      ('dim_date', 'silver_date'),
      ('dim_time', 'silver_time'),
      ('dim_statustype', 'silver_status_type'),
      ('dim_tradetype', 'silver_trade_type'),
      ('dim_broker', 'silver_hr'),
      ('dim_customer', 'silver_customer'),
      ('dim_account', 'silver_account'),
      ('dim_prospect', 'silver_prospect')
    ] %}
    {% for gold_name, silver_name in direct_dims %}
      {{ log_gold_reconciliation(
          gold_name,
          "select count(*) from " ~ ref(silver_name),
          ref(gold_name),
          2,
          true
      ) }}
    {% endfor %}

    {# ---- Dims: dedup_latest collapse (many versions -> one row per key), + Unknown row ---- #}
    {% set dedup_dims = [
      ('dim_company', 'silver_finwire_company'),
      ('dim_security', 'silver_finwire_security')
    ] %}
    {% for gold_name, silver_name in dedup_dims %}
      {{ log_gold_reconciliation(
          gold_name,
          "select count(*) from " ~ ref(silver_name),
          ref(gold_name),
          60,
          true
      ) }}
    {% endfor %}

    {# ---- Facts: direct pass-through, FK resolution via left join only ---- #}
    {% set direct_facts = [
      ('fact_trade', 'silver_trade'),
      ('fact_cashtransaction', 'silver_cash_transaction'),
      ('fact_watchitem', 'silver_watch_history'),
      ('fact_market_history', 'silver_daily_market'),
      ('fact_company_financials', 'silver_finwire_financials')
    ] %}
    {% for gold_name, silver_name in direct_facts %}
      {{ log_gold_reconciliation(
          gold_name,
          "select count(*) from " ~ ref(silver_name),
          ref(gold_name),
          2,
          false
      ) }}
    {% endfor %}

    {# ---- Facts: inner join to fact_trade for derived FKs, can drop orphans ---- #}
    {% set lookup_facts = [
      ('fact_holding', 'silver_holding_history'),
      ('fact_trade_history', 'silver_trade_history')
    ] %}
    {% for gold_name, silver_name in lookup_facts %}
      {{ log_gold_reconciliation(
          gold_name,
          "select count(*) from " ~ ref(silver_name),
          ref(gold_name),
          15,
          false
      ) }}
    {% endfor %}

  {% endif %}
{% endmacro %}