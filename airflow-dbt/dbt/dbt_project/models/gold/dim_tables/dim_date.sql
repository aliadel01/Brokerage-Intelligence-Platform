{#-
    Archetype A pass-through. date_sk is NOT hashed -- it's the natural
    key carried straight from silver_date (ultimately bronze's
    sk_dateid), per gold.md ADR-007's smart-key exception.
-#}

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