-- tests/assert_forward_fill_sanity_customer.sql
-- This test checks that for each customer_id, if a non-null value exists in a previous record, 
-- then the current record should not have a null value for the same field.
with fill_checks as (
    select
        customer_id,
        valid_from_date,
        status_id    
        last_name,    
        first_name,   
        tier,    
        address_line1,
        city, 
        state_province,
        country, 
        primary_email,
        lag(status_id)      over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_status_id,
        lag(last_name)      over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_last_name,
        lag(first_name)     over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_first_name,
        lag(tier)           over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_tier,
        lag(address_line1)  over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_address_line1,
        lag(city)           over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_city,
        lag(state_province) over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_state_province,
        lag(country)        over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_country,
        lag(primary_email)  over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_primary_email
    from {{ ref('silver_customer') }}
)

select *
from fill_checks
where (prev_status_id is not null and status_id is null)
   or (prev_last_name is not null and last_name is null) 
   or (prev_first_name is not null and first_name is null)
    or (prev_tier is not null and tier is null)
    or (prev_address_line1 is not null and address_line1 is null)
    or (prev_city is not null and city is null)
    or (prev_state_province is not null and state_province is null)
    or (prev_country is not null and country is null)
    or (prev_primary_email is not null and primary_email is null)