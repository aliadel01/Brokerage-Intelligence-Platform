{#-
    Archetype A pass-through. time_sk is NOT hashed -- same smart-key
    exception as dim_date, per gold.md ADR-007.
-#}

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