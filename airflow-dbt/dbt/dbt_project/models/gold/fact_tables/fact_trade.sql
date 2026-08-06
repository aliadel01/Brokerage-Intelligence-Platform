{#-
    trade_sk is NOT hashed -- it's trade_id directly, per gold.md
    ADR-007 (trade_id confirmed globally unique by a real query).

    account_sk resolved with a TIME-AWARE join against dim_account
    (the account version valid as of trade_timestamp), same reasoning
    as dim_account.sql's join to dim_customer.

    trade_date_sk/trade_time_sk resolved by VALUE match against
    dim_date/dim_time, not by constructing an assumed key encoding --
    date_sk/time_sk are smart keys carried from the source (ADR-007),
    and their exact encoding isn't something to guess at here.
-#}

with trade as (
    select * from {{ ref('silver_trade') }}
),

security as (
    select security_sk, symbol from {{ ref('dim_security') }}
),

account as (
    select account_sk, account_id, effective_start_date, effective_end_date
    from {{ ref('dim_account') }}
),

status_type as (
    select status_type_sk, status_code from {{ ref('dim_statustype') }}
),

trade_type as (
    select trade_type_sk, trade_type_code from {{ ref('dim_tradetype') }}
),

trade_date as (
    select date_sk, date_value from {{ ref('dim_date') }}
),

trade_time as (
    select time_sk, time_value from {{ ref('dim_time') }}
)

select
    trade.trade_id                as trade_sk,
    trade.trade_id,
    security.security_sk,
    trade_date.date_sk            as trade_date_sk,
    trade_time.time_sk            as trade_time_sk,
    status_type.status_type_sk,
    trade_type.trade_type_sk,
    trade.is_cash                 as is_cash_flag,
    trade.quantity,
    trade.bid_price,
    account.account_sk,
    trade.execution_name          as executor_name,
    trade.trade_price,
    trade.charge                  as charge_amount,
    trade.commission               as commission_amount,
    trade.tax                     as tax_amount,
    trade.cdc_flag,
    trade._batch_id               as batch_id
from trade
left join security
    on trade.symbol = security.symbol
left join account
    on trade.customer_account_id = account.account_id
    and trade.trade_timestamp::date >= account.effective_start_date
    and trade.trade_timestamp::date <= coalesce(account.effective_end_date, date '9999-12-31')
left join status_type
    on trade.status_id = status_type.status_code
left join trade_type
    on trade.trade_type_id = trade_type.trade_type_code
left join trade_date
    on trade_date.date_value = trade.trade_timestamp::date
left join trade_time
    on trade_time.time_value = trade.trade_timestamp::time