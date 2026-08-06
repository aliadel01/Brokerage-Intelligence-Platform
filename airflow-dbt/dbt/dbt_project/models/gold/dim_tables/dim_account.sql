{#-
    Grain: One row per account version (SCD Type 2).

    SCD2 pass-through model: silver_account handles SCD Type 2 logic and
    generates account_version_sk. This model passes through account_version_sk
    as account_sk to preserve surrogate key determinism across gold pipelines.

    Key Resolutions:
    - broker_sk: Resolved via equi-join against dim_broker (Type 1 dimension).
    - customer_sk: Resolved via a TIME-AWARE join against dim_customer (SCD Type 2),
      matching account.valid_from_date against customer.effective_start_date and
      customer.effective_end_date to maintain historical point-in-time accuracy.
-#}

with account as (
    select * from {{ ref('silver_account') }}
),

broker as (
    select broker_sk, employee_id from {{ ref('dim_broker') }}
),

customer as (
    select customer_sk, customer_id, effective_start_date, effective_end_date
    from {{ ref('dim_customer') }}
),


final as (
    select
        account.account_version_sk as account_sk,
        account.account_id,
        account.account_name,
        account.tax_status,
        account.status_id                                as account_status,
        broker.broker_sk,
        customer.customer_sk,
        account.valid_from_date                          as effective_start_date,
        nullif(account.valid_to_date, date '9999-12-31') as effective_end_date,
        account.is_current,
        account.cdc_flag
    from account
    left join broker
        on account.broker_id = broker.employee_id
    left join customer
        on account.customer_id = customer.customer_id
        and account.valid_from_date >= customer.effective_start_date
        and account.valid_from_date <= coalesce(customer.effective_end_date, date '9999-12-31')
)
select * from final