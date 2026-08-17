{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='silver.silver_finwire_security',
      tags={
        'posting_ts': 'internal',
        'security_symbol': 'public',
        'issue_type': 'public',
        'status': 'public',
        'security_name': 'public',
        'exchange_id': 'public',
        'shares_outstanding': 'public',
        'first_trade_date': 'public',
        'first_trade_exchange_date': 'public',
        'dividend': 'public',
        'company_name': 'public',
        'company_cik': 'public',
        '_batch_id': 'internal',
        '_row_hash': 'internal',
        '_loaded_at': 'internal'
      }
    )
) }}
{#-
    Grain: One row per (security_symbol, posting_ts).
    
    Preserves full append-only history of security states across time.
    Deduplicates only physical ingestion retries using _loaded_at desc.
    
    coname/cocik are resolved upstream in bronze (_resolve_co_name_or_cik)
    and passed through as-is.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_finwire_sec') }}
),

cleaned as (
    select
        pts                                                 as posting_ts,
        {{ trim_or_null('symbol', uppercase=true) }}       as security_symbol,
        {{ trim_or_null('issuetype', uppercase=true) }}    as issue_type,
        {{ trim_or_null('status', uppercase=true) }}       as status,
        {{ trim_or_null('name') }}                          as security_name,
        {{ trim_or_null('exid', uppercase=true) }}         as exchange_id,
        shout                                                as shares_outstanding,
        firsttradedate                                       as first_trade_date,
        firsttradeexchg                                      as first_trade_exchange_date,
        dividend,
        {{ trim_or_null('coname') }}                        as company_name,
        {{ trim_or_null('cocik') }}                         as company_cik,

        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where pts is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'security_symbol, posting_ts', '_loaded_at desc') }}
)

select * from deduped
