{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='gold.dim_time',
      tags={
        'time_sk': 'public',
        'time_value': 'public',
        'hour_id': 'public',
        'hour_desc': 'public',
        'minute_id': 'public',
        'minute_desc': 'public',
        'second_id': 'public',
        'second_desc': 'public',
        'is_market_hours': 'public',
        'is_office_hours': 'public'
      }
    )
) }}
{#-
    Archetype A pass-through, plus one Unknown member row (time_sk = -1).
-#}

with final as (
    select
        cast(time_sk as integer)                    as time_sk,
        to_time(time_value)                         as time_value,
        cast(hour_id as integer)                    as hour_id,
        cast(hour_desc as varchar)                  as hour_desc,
        cast(minute_id as integer)                  as minute_id,
        cast(minute_desc as varchar)                as minute_desc,
        cast(second_id as integer)                  as second_id,
        cast(second_desc as varchar)                as second_desc,
        cast(market_hours_flag as boolean)          as is_market_hours,
        cast(office_hours_flag as boolean)          as is_office_hours
    from {{ ref('silver_time') }}
),

unknown as (
    select
        -1                  as time_sk,
        cast(null as time)  as time_value,
        -1                  as hour_id,
        'Unknown'           as hour_desc,
        -1                  as minute_id,
        'Unknown'           as minute_desc,
        -1                  as second_id,
        'Unknown'           as second_desc,
        false               as is_market_hours,
        false               as is_office_hours
)

select * from final
union all
select * from unknown
