-- tests/silver_trade_history/assert_trade_status_order.sql
with ordered as (
  select trade_id, status_id, status_ts,
         lag(status_id) over (partition by trade_id order by status_ts) as prev_status
  from {{ ref('silver_trade_history') }}
)
select *
from ordered
where (prev_status, status_id) not in (

  ('PNDG','SBMT'), ('PNDG','CANC'), ('SBMT','CMPT'), ('SBMT','CANC'), ('CMPT','INAC'), ('CANC','INAC')

)
and prev_status is not null