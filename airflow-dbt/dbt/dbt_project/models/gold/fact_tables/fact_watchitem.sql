with watch as (
    select * from {{ ref('silver_watch_history') }}
),

customer as (
    select customer_sk, customer_id, effective_start_date, effective_end_date
    from {{ ref('dim_customer') }}
),

security as (
    select security_sk, symbol from {{ ref('dim_security') }}
),

watch_date as (
    select date_sk, date_value from {{ ref('dim_date') }}
),

watch_time as (
    select time_sk, time_value from {{ ref('dim_time') }}
)

select
    customer.customer_sk,
    security.security_sk,
    watch_date.date_sk    as watch_date_sk,
    watch_time.time_sk    as watch_time_sk,
    watch.watch_action    as action_code,
    watch._cdc_flag       as cdc_flag,
    watch._cdc_dsn        as cdc_dsn,
    watch._batch_id       as batch_id
from watch
left join customer
    on watch.customer_id = customer.customer_id
    and watch.event_ts::date >= customer.effective_start_date
    and watch.event_ts::date <= coalesce(customer.effective_end_date, date '9999-12-31')
left join security
    on watch.security_symbol = security.symbol
left join watch_date
    on watch_date.date_value = watch.event_ts::date
left join watch_time
    on watch_time.time_value = watch.event_ts::time