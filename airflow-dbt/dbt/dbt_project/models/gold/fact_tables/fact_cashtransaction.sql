with cash as (
    select * from {{ ref('silver_cash_transaction') }}
),

account as (
    select account_sk, account_id, effective_start_date, effective_end_date
    from {{ ref('dim_account') }}
),

txn_date as (
    select date_sk, date_value from {{ ref('dim_date') }}
),

txn_time as (
    select time_sk, time_value from {{ ref('dim_time') }}
)

select
    txn_date.date_sk       as transaction_date_sk,
    txn_time.time_sk       as transaction_time_sk,
    account.account_sk,
    cash.amount,
    cash.description,
    cash._cdc_flag         as cdc_flag,
    cash._cdc_dsn          as cdc_dsn,
    cash._batch_id         as batch_id
from cash
left join account
    on cash.account_id = account.account_id
    and cash.transaction_ts::date >= account.effective_start_date
    and cash.transaction_ts::date <= coalesce(account.effective_end_date, date '9999-12-31')
left join txn_date
    on txn_date.date_value = cash.transaction_ts::date
left join txn_time
    on txn_time.time_value = cash.transaction_ts::time