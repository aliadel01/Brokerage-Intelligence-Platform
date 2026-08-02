{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_date') }}
),

cleaned as (
    select
        sk_dateid                                   as date_sk,
        datevalue                                    as date_value,
        {{ trim_or_null('datedesc') }}               as date_desc,
        calendaryearid                                as calendar_year_id,
        {{ trim_or_null('calendaryeardesc') }}       as calendar_year_desc,
        calendarqtrid                                 as calendar_qtr_id,
        {{ trim_or_null('calendarqtrdesc') }}        as calendar_qtr_desc,
        calendarmonthid                               as calendar_month_id,
        {{ trim_or_null('calendarmonthdesc') }}      as calendar_month_desc,
        calendarweekid                                as calendar_week_id,
        {{ trim_or_null('calendarweekdesc') }}       as calendar_week_desc,
        dayofweeknum                                  as day_of_week_num,
        {{ trim_or_null('dayofweekdesc') }}          as day_of_week_desc,
        fiscalyearid                                  as fiscal_year_id,
        {{ trim_or_null('fiscalyeardesc') }}         as fiscal_year_desc,
        fiscalqtrid                                   as fiscal_qtr_id,
        {{ trim_or_null('fiscalqtrdesc') }}          as fiscal_qtr_desc,
        holidayflag                                   as holiday_flag,

        _batch_id,
        _loaded_at
    from source
    where sk_dateid is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'date_sk', '_loaded_at desc') }}
)

select * from deduped