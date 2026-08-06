{#-
    Per gold.md ADR-005: account_sk/security_sk/holding_date_sk resolved
    entirely from fact_trade via the CURRENT/triggering trade_id
    (HH_T_ID -> silver's `trade_id`), not originating_trade_id -- that
    column is informational-only per ADR-006 and never used for
    resolution. INNER JOIN is deliberate: this model must run after
    fact_trade.
-#}

with holding as (
    select * from {{ ref('silver_holding_history') }}
),

trade as (
    select trade_sk, trade_id, account_sk, security_sk, trade_date_sk from {{ ref('fact_trade') }}
)

select
    holding.originating_trade_id,
    holding.trade_id       as current_trade_id,
    trade.security_sk,
    trade.trade_date_sk    as holding_date_sk,
    trade.account_sk,
    holding.qty_before     as before_quantity,
    holding.qty_after      as after_quantity,
    holding._cdc_flag      as cdc_flag,
    holding._cdc_dsn       as cdc_dsn,
    holding._batch_id      as batch_id
from holding
inner join trade
    on holding.trade_id = trade.trade_id