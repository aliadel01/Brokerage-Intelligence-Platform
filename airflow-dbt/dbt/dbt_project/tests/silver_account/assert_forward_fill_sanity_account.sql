-- tests/assert_forward_fill_sanity_account.sql
-- This test checks that for each account_id, if a non-null value exists in a previous record, 
-- then the current record should not have a null value for the same field. 
-- This ensures that forward filling has been done correctly.
with fill_checks as (
    select
        account_id,
        valid_from_date,
        status_id,
        account_name,
        lag(status_id) ignore nulls over (
            partition by account_id 
            order by valid_from_date
        ) as prev_non_null_status,
        lag(account_name) ignore nulls over (
            partition by account_id 
            order by valid_from_date
        ) as prev_non_null_name
    from {{ ref('silver_account') }}
)

select *
from fill_checks
where (prev_non_null_status is not null and status_id is null)
   or (prev_non_null_name is not null and account_name is null)