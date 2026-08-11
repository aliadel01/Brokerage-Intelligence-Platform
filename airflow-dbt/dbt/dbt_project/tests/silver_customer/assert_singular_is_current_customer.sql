-- tests/assert_singular_is_current_customer.sql
-- This test checks that for each customer_id, there is exactly one record where is_current is true.
select
    customer_id,
    count_if(is_current = true) as current_records_count
from {{ ref('silver_customer') }}
group by customer_id
having count_if(is_current = true) != 1