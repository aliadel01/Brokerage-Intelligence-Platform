{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='silver.silver_industry',
      tags={
        'industry_id': 'public',
        'industry_name': 'public',
        'sector_id': 'public',
        '_batch_id': 'internal',
        '_loaded_at': 'internal'
      }
    )
) }}
{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_industry') }}
),

cleaned as (
    select
        {{ trim_or_null('in_id', uppercase=true) }}    as industry_id,
        {{ trim_or_null('in_name') }}                  as industry_name,
        {{ trim_or_null('in_sc_id', uppercase=true) }} as sector_id,

        _batch_id,
        _loaded_at
    from source
    where in_id is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'industry_id', '_loaded_at desc') }}
)

select * from deduped
