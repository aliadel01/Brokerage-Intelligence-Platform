{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='gold.fact_holding',
      tags={
        'originating_trade_id': 'internal',
        'current_trade_id': 'internal',
        'security_sk': 'internal',
        'holding_date_sk': 'internal',
        'account_sk': 'internal',
        'before_quantity': 'confidential',
        'after_quantity': 'confidential',
        'batch_id': 'internal'
      }
    )
) }}
{#-
    account_sk/security_sk/holding_date_sk resolved from fact_trade via
    CURRENT/triggering trade_id (HH_T_ID -> silver's trade_id), per
    ADR-005/ADR-006. INNER JOIN deliberate (must run after fact_trade).
    fact_trade's account_sk/security_sk already coalesced to -1
    upstream -- no re-coalesce needed here.
-#}

with holding as (
    select * from {{ ref('silver_holding_history') }}
),

trade as (
    select trade_sk, trade_id, account_sk, security_sk, trade_date_sk from {{ ref('fact_trade') }}
),

resolved as (
    select
        holding.originating_trade_id,
        holding.trade_id       as current_trade_id,
        trade.security_sk,
        trade.trade_date_sk    as holding_date_sk,
        trade.account_sk,
        holding.qty_before     as before_quantity,
        holding.qty_after      as after_quantity,
        holding._batch_id      as batch_id
    from holding
    inner join trade
        on holding.trade_id = trade.trade_id
)

select
    {{ gen_surrogate_key(['originating_trade_id', 'current_trade_id', 'security_sk', 'account_sk', 'holding_date_sk']) }} as holding_sk,
    *
from resolved
