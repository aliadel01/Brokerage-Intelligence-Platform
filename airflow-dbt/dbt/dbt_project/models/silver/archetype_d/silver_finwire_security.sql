{#-
    Archetype D (parsed): Batch1 only, append-only-by-PTS, no CDC columns
    in the source file at all. coname/cocik are already resolved into two
    mutually-exclusive columns upstream in bronze (see ingestion layer
    doc, _resolve_co_name_or_cik) -- passed through as-is here.
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