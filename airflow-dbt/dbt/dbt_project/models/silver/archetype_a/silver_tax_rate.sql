{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_tax_rate') }}
),

cleaned as (
    select
        {{ trim_or_null('tx_id', uppercase=true) }}   as tax_rate_id,
        {{ trim_or_null('tx_name') }}                 as tax_rate_name,
        tx_rate                                          as tax_rate,

        _batch_id,
        _loaded_at
    from source
    where tx_id is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'tax_rate_id', '_loaded_at desc') }}
)

select * from deduped