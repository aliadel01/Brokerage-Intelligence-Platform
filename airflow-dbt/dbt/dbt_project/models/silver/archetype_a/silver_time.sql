{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='silver.silver_time',
      tags={
        'time_sk': 'public',
        'time_value': 'public',
        'hour_id': 'public',
        'hour_desc': 'public',
        'minute_id': 'public',
        'minute_desc': 'public',
        'second_id': 'public',
        'second_desc': 'public',
        'market_hours_flag': 'public',
        'office_hours_flag': 'public',
        '_batch_id': 'internal',
        '_loaded_at': 'internal'
      }
    )
) }}
{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_time') }}
),

cleaned as (
    select
        sk_timeid                                    as time_sk,
        {{ trim_or_null('timevalue') }}              as time_value,
        hourid                                         as hour_id,
        {{ trim_or_null('hourdesc') }}               as hour_desc,
        minuteid                                       as minute_id,
        {{ trim_or_null('minutedesc') }}             as minute_desc,
        secondid                                       as second_id,
        {{ trim_or_null('seconddesc') }}             as second_desc,
        markethoursflag                                as market_hours_flag,
        officehoursflag                                as office_hours_flag,

        _batch_id,
        _loaded_at
    from source
    where sk_timeid is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'time_sk', '_loaded_at desc') }}
)

select * from deduped
