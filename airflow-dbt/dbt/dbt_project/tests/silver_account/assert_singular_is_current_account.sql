-- tests/assert_singular_is_current.sql
-- This test checks that for each account_id, there is exactly one record where is_current is true.
select
    account_id,
    count_if(is_current = true) as current_records_count
from {{ ref('silver_account') }}
group by account_id
having count_if(is_current = true) != 1