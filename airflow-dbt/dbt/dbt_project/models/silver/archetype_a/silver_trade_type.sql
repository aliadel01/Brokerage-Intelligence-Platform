{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
    tt_is_sell / tt_is_mrkt arrive from bronze as NUMBER(1) flags (kept
    1:1 with the source file there, per the bronze DDL comment). Casting
    them to native BOOLEAN is the silver-layer decision referenced in
    that comment, done here.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_trade_type') }}
),

cleaned as (
    select
        {{ trim_or_null('tt_id', uppercase=true) }}   as trade_type_id,
        {{ trim_or_null('tt_name') }}                 as trade_type_name,
        (tt_is_sell = 1)                                as is_sell_flag,
        (tt_is_mrkt = 1)                                as is_market_flag,

        _batch_id,
        _loaded_at
    from source
    where tt_id is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'trade_type_id', '_loaded_at desc') }}
)

select * from deduped