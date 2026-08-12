-- tests/silver_customer/assert_customer_versions_only_on_tracked_change.sql
with v as (
  select customer_id, status_id, last_name, first_name, tier, address_line1, city, state_province, country, primary_email, _batch_id,
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
from v
where prev_status_id is not null
  and (status_id, last_name, first_name, tier, address_line1, city, state_province, country, primary_email) is not distinct from (prev_status_id, prev_last_name, prev_first_name, prev_tier, prev_address_line1, prev_city, prev_state_province, prev_country, prev_primary_email)