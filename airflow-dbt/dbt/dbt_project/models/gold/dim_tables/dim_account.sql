{#-
    Grain: One row per account version (SCD Type 2), plus one Unknown
    member row (account_sk = -1) for fact FK coalesce targets.
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
        coalesce(account.status_id, 'Unknown')            as account_status,
        broker.broker_sk,
        customer.customer_sk,
        account.valid_from_date                           as effective_start_date,
        nullif(account.valid_to_date, date '9999-12-31')  as effective_end_date,
        account.is_current,
        account.cdc_flag
    from account
    left join broker
        on account.broker_id = broker.employee_id
    left join customer
        on account.customer_id = customer.customer_id
        and account.valid_from_date >= customer.effective_start_date
        and account.valid_from_date <= coalesce(customer.effective_end_date, date '9999-12-31')
),

unknown as (
    select
        '-1'                 as account_sk,
        -1                  as account_id,
        'Unknown'           as account_name,
        'Unknown'           as tax_status,
        'Unknown'           as account_status,
        '-1'                as broker_sk,
        '-1'                as customer_sk,
        date '1900-01-01'   as effective_start_date,
        cast(null as date)  as effective_end_date,
        false               as is_current,
        'U'                 as cdc_flag
)

select * from final
union all
select * from unknown