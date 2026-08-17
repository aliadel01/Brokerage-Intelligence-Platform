{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='silver.silver_trade_history',
      tags={
        'trade_id': 'internal',
        'status_ts': 'internal',
        'status_id': 'internal',
        '_batch_id': 'internal',
        '_row_hash': 'internal',
        '_loaded_at': 'internal',
        '_source_model': 'internal'
      }
    )
) }}
{#-
    Per 04_silver.md ADR-002: Trade is split by concern into two models.
    This one owns the FULL cross-batch status-transition history for
    Trade, unifying two bronze sources:

    - bronze_trade_history: Batch1 only. Carries the complete lifecycle
      of every Batch1 trade (Archetype E per 02_bronze_design.md).
    - bronze_trade: Batch2/3 CDC events. Each distinct status change
      arrives as its own row. Batch1 rows in bronze_trade are excluded
      here -- a real query confirmed TRADE_ID is unique in Batch1 within
      bronze_trade, meaning it carries only each trade's final status
      for Batch1, not its lifecycle. Unioning it in would either exactly
      duplicate or silently conflict with the already-complete Batch1
      lifecycle bronze_trade_history already provides, and neither
      source alone holds the complete Batch1 picture without the other,
      so exclusion is the only resolution -- not a judgment call.

    Batch2/3 CDC events are filtered down to real status TRANSITIONS
    only -- a status re-sent unchanged is not a new history row.

    _source_model added for lineage: distinguishes which bronze table a
    given transition row came from, since _batch_id alone conflates
    "Batch1" with "came from bronze_trade_history" only by convention,
    not by a hard guarantee.
-#}

with history_batch1 as (
    select
        th_t_id                                        as trade_id,
        th_dts                                          as status_ts,
        {{ trim_or_null('th_st_id', uppercase=true) }} as status_id,

        _batch_id,
        _row_hash,
        _loaded_at,
        'bronze_trade_history'                          as _source_model
    from {{ source('bronze', 'bronze_trade_history') }}
    where th_t_id is not null
      and th_dts is not null
),

trade_events_batch23 as (
    select
        t_id                                            as trade_id,
        t_dts                                            as status_ts,
        {{ trim_or_null('t_st_id', uppercase=true) }}  as status_id,

        _cdc_dsn,
        _batch_id,
        _row_hash,
        _loaded_at,
        'bronze_trade'                                   as _source_model
    from {{ source('bronze', 'bronze_trade') }}
    where t_id is not null
      and t_dts is not null
      and _batch_id != 1   -- Batch1 excluded: see model note above
),

-- Only real status changes count as a transition -- reject a CDC event
-- that re-sends the same status as the immediately preceding one.
trade_changed_only as (
    select
        *,
        lag(status_id) over (
            partition by trade_id
            order by _batch_id, status_ts, _cdc_dsn, _loaded_at
        ) as prev_status_id
    from trade_events_batch23
),

trade_transitions_batch23 as (
    select
        trade_id,
        status_ts,
        status_id,
        _batch_id,
        _row_hash,
        _loaded_at,
        _source_model
    from trade_changed_only
    where prev_status_id is null
       or status_id is distinct from prev_status_id
),

unioned as (
    select trade_id, status_ts, status_id, _batch_id, _row_hash, _loaded_at, _source_model
    from history_batch1

    union all

    select trade_id, status_ts, status_id, _batch_id, _row_hash, _loaded_at, _source_model
    from trade_transitions_batch23
),

-- Defense-in-depth only: drops true duplicate ingestion. Every distinct
-- (trade_id, status_ts, status_id) survives this step.
deduped as (
    {{ dedup_latest('unioned', 'trade_id, status_ts, status_id', '_loaded_at desc') }}
)

select * from deduped
