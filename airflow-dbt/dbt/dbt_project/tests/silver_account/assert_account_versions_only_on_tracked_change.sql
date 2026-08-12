-- tests/silver_account/assert_account_versions_only_on_tracked_change.sql
with v as (
  select account_id, status_id, account_name, _batch_id,
         lag(status_id) over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at) as prev_status_id,
         lag(account_name) over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at) as prev_account_name
  from {{ ref('silver_account') }}
)
select *
from v
where prev_status_id is not null
  and (status_id, account_name) is not distinct from (prev_status_id, prev_account_name)