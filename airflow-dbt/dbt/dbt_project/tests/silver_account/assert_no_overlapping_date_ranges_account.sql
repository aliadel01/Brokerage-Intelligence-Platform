-- tests/assert_no_overlapping_date_ranges.sql
-- This test checks that for each account_id, the valid_from_date and valid_to_date ranges do not overlap.
-- Returns any record where the valid_from_date of a newer version is less than or equal to the valid_to_date of the previous version.
with ordered_versions as (
    select
        account_id,
        valid_from_date,
        valid_to_date,
        lag(valid_to_date) over (
            partition by account_id 
            order by valid_from_date
        ) as prev_valid_to_date
    from {{ ref('silver_account') }}
)

select *
from ordered_versions
where prev_valid_to_date is not null
  and valid_from_date <= prev_valid_to_date