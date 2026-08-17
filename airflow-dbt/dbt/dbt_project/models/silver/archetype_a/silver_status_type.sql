{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='silver.silver_status_type',
      tags={
        'status_id': 'public',
        'status_name': 'public',
        '_batch_id': 'internal',
        '_loaded_at': 'internal'
      }
    )
) }}
{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_status_type') }}
),

cleaned as (
    select
        {{ trim_or_null('st_id', uppercase=true) }}   as status_id,
        {{ trim_or_null('st_name') }}                 as status_name,

        _batch_id,
        _loaded_at
    from source
    where st_id is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'status_id', '_loaded_at desc') }}
)

select * from deduped
