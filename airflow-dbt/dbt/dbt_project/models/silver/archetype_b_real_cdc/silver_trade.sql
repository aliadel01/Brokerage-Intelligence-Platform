{{ config(
    materialized='table',
    post_hook=apply_pii_masking(
      string_cols=['execution_name']
    )
) }}
{#-
    Per 04_silver.md ADR-002: Trade is split by concern into two models.
    This one owns the latest known STATE of a trade -- grain is one row
    per trade_id. Full status-transition history is owned separately by
    silver_trade_history.

    Dedup shape: state-tracking (ADR-002), same shape as silver_prospect
    -- one row per business key, latest wins. Ordered by _batch_id desc,
    trade_timestamp desc, _cdc_dsn desc, _loaded_at desc.

    Every column here is simply "latest known value" for that trade_id.
-#}

with source as (
    select
        t_id                                                as trade_id,
        t_dts                                                as trade_timestamp,
        {{ trim_or_null('t_st_id', uppercase=true) }}      as status_id,
        {{ trim_or_null('t_tt_id', uppercase=true) }}      as trade_type_id,
        t_is_cash                                            as is_cash,
        {{ trim_or_null('t_s_symb', uppercase=true) }}     as symbol,
        t_qty                                                as quantity,
        t_bid_price                                          as bid_price,
        t_ca_id                                              as customer_account_id,
        {{ trim_or_null('t_exec_name') }}                  as execution_name,
        t_trade_price                                        as trade_price,
        t_chrg                                               as charge,
        t_comm                                               as commission,
        t_tax                                                as tax,
        {{ trim_or_null('_cdc_flag', uppercase=true) }}    as cdc_flag,
        _cdc_dsn,
        _batch_id,
        _source_file,
        _loaded_at,
        _row_hash
    from {{ source('bronze', 'bronze_trade') }}
    where t_id is not null
      and t_dts is not null
),

deduped as (
    {{ dedup_latest('source', 'trade_id', '_batch_id desc, trade_timestamp desc, _cdc_dsn desc, _loaded_at desc') }}
)

select
    trade_id,
    status_id,
    trade_type_id,
    is_cash,
    symbol,
    quantity,
    bid_price,
    customer_account_id,
    execution_name,
    trade_price,
    charge,
    commission,
    tax,
    cdc_flag,
    trade_timestamp,
    _batch_id
from deduped