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
),

resolved as (
    select
        coalesce(txn_date.date_sk, -1)    as transaction_date_sk,
        coalesce(txn_time.time_sk, -1)    as transaction_time_sk,
        coalesce(account.account_sk, -1)  as account_sk,
        cash.amount,
        cash.description,
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
)

select
    {{ gen_surrogate_key(['account_sk', 'transaction_date_sk', 'transaction_time_sk']) }} as cash_transaction_sk,
    *
from resolved