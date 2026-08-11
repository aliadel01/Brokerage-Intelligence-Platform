-- tests/assert_scd2_date_continuity_account.sql
-- This test checks that for each account_id, the valid_from_date and valid_to_date ranges are continuous.
-- Returns any record where the valid_to_date of a version is not exactly one day before the valid_from_date of the next version.
with version_chain as (
    select
        account_id,
        valid_from_date,
        valid_to_date,
        lead(valid_from_date) over (
            partition by account_id 
            order by valid_from_date
        ) as next_valid_from_date
    from {{ ref('silver_account') }}
)

select *
from version_chain
where next_valid_from_date is not null
  and valid_to_date != dateadd(day, -1, next_valid_from_date)