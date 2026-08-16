{{ config(
    materialized='table',
    post_hook=apply_pii_masking(
      string_cols=['first_name','last_name','middle_initial','phone_number']
    )
) }}
with final as (
    select
        {{ gen_surrogate_key(['employee_id']) }} as broker_sk,
        employee_id,
        manager_id                         as manager_employee_id,
        first_name,
        last_name,
        middle_initial,
        coalesce(job_code, -1)      as job_code,
        coalesce(branch, 'Unknown')        as branch_name,
        coalesce(office, 'Unknown')        as office_code,
        phone                              as phone_number
    from {{ ref('silver_hr') }}
),

unknown as (
    select
        '-1'                    as broker_sk,
        cast(null as varchar) as employee_id,
        cast(null as varchar) as manager_employee_id,
        'Unknown'             as first_name,
        'Unknown'             as last_name,
        cast(null as varchar) as middle_initial,
        -1                    as job_code,
        'Unknown'             as branch_name,
        'Unknown'             as office_code,
        cast(null as varchar) as phone_number
)

select * from final
union all
select * from unknown