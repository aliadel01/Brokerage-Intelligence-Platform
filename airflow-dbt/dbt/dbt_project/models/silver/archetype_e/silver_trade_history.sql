{#-
    Archetype E: Batch1-only fact data (no Batch2/3 counterpart at all, per
    spec). Multiple lifecycle-status rows exist per trade_id, so the
    natural key is the combination of trade + timestamp + status, not
    trade_id alone. Dedup only removes exact duplicate ingestion.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_trade_history') }}
),

cleaned as (
    select
        th_t_id                                        as trade_id,
        th_dts                                          as status_ts,
        {{ trim_or_null('th_st_id', uppercase=true) }} as status_id,

        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where th_t_id is not null
      and th_dts is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'trade_id, status_ts, status_id', '_loaded_at desc') }}
)

select * from deduped