{#-
    account_sk/security_sk resolved via lookup to fact_trade on trade_id
    (ADR-010). INNER JOIN to fact_trade deliberate (must run after
    fact_trade). fact_trade's own account_sk/security_sk already
    coalesced to -1 upstream, so no re-coalesce needed on those two.
    status_type_sk/status_date_sk/status_time_sk resolved separately
    here and still need their own coalesce.
    trade_history_sk hashed on the grain columns (trade_id +
    status_date_sk + status_time_sk + status_type_sk).
-#}

with trade_history as (
    select * from {{ ref('silver_trade_history') }}
),

trade as (
    select trade_sk, trade_id, account_sk, security_sk from {{ ref('fact_trade') }}
),

status_type as (
    select status_type_sk, status_code from {{ ref('dim_statustype') }}
),

status_date as (
    select date_sk, date_value from {{ ref('dim_date') }}
),

status_time as (
    select time_sk, time_value from {{ ref('dim_time') }}
),

resolved as (
    select
        trade.trade_sk,
        trade_history.trade_id,
        trade.account_sk,
        trade.security_sk,
        coalesce(status_type.status_type_sk, -1)  as status_type_sk,
        coalesce(status_date.date_sk, -1)         as status_date_sk,
        coalesce(status_time.time_sk, -1)         as status_time_sk,
        trade_history._source_model                as source_model,
        trade_history._batch_id                    as batch_id
    from trade_history
    inner join trade
        on trade_history.trade_id = trade.trade_id
    left join status_type
        on trade_history.status_id = status_type.status_code
    left join status_date
        on status_date.date_value = trade_history.status_ts::date
    left join status_time
        on status_time.time_value = trade_history.status_ts::time
)

select
    {{ gen_surrogate_key(['trade_id', 'status_date_sk', 'status_time_sk', 'status_type_sk']) }} as trade_history_sk,
    *
from resolved