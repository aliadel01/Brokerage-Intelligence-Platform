{#-
    Archetype A pass-through, plus one Unknown member row (date_sk = -1).
-#}

with final as (
    select
        date_sk,
        date_value,
        date_desc,
        calendar_year_id      as calendar_year,
        calendar_year_desc,
        calendar_qtr_id,
        calendar_qtr_desc,
        calendar_month_id,
        calendar_month_desc,
        calendar_week_id,
        calendar_week_desc,
        day_of_week_num,
        day_of_week_desc,
        fiscal_year_id         as fiscal_year,
        fiscal_year_desc,
        fiscal_qtr_id,
        fiscal_qtr_desc,
        holiday_flag           as is_holiday
    from {{ ref('silver_date') }}
),

unknown as (
    select
        -1                  as date_sk,
        date '1900-01-01'   as date_value,
        'Unknown'           as date_desc,
        -1                  as calendar_year,
        'Unknown'           as calendar_year_desc,
        -1                  as calendar_qtr_id,
        'Unknown'           as calendar_qtr_desc,
        -1                  as calendar_month_id,
        'Unknown'           as calendar_month_desc,
        -1                  as calendar_week_id,
        'Unknown'           as calendar_week_desc,
        -1                  as day_of_week_num,
        'Unknown'           as day_of_week_desc,
        -1                  as fiscal_year,
        'Unknown'           as fiscal_year_desc,
        -1                  as fiscal_qtr_id,
        'Unknown'           as fiscal_qtr_desc,
        false               as is_holiday
)

select * from final
union all
select * from unknown