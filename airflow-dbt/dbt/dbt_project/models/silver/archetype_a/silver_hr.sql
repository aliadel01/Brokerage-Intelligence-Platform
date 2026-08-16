{{ config(
    materialized='table',
    post_hook=apply_pii_masking(
      string_cols=['first_name','last_name','middle_initial','phone']
    )
) }}
{#-
    Archetype A: static reference dimension, Batch1 only, no CDC.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_hr') }}
),

cleaned as (
    select
        employeeid                                      as employee_id,
        managerid                                        as manager_id,
        {{ trim_or_null('employeefirstname') }}        as first_name,
        {{ trim_or_null('employeelastname') }}         as last_name,
        {{ trim_or_null('employeemi', uppercase=true) }} as middle_initial,
        employeejobcode                                  as job_code,
        {{ trim_or_null('employeebranch') }}           as branch,
        {{ trim_or_null('employeeoffice') }}           as office,
        {{ trim_or_null('employeephone') }}            as phone,

        _batch_id,
        _loaded_at
    from source
    where employeeid is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'employee_id', '_loaded_at desc') }}
)

select * from deduped