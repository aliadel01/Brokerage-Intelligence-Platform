{#-
    SCD Type 2 for Trade, single source (bronze_trade, real CDC -- no XML
    counterpart, unlike account/customer).

    GRAIN (04_silver.md decision #17): one row per trade_id per EVENT,
    using trade_timestamp (t_dts) itself as the valid_from boundary --
    NOT _batch_id. A trade can move through several lifecycle statuses
    inside a single batch (e.g. submitted then completed same day), so
    batch-level granularity (fine for account/customer) would collapse
    real same-day status changes into one row here. Kept ONLY if a
    tracked column differs from the immediately preceding EVENT (not
    preceding batch).

    Fixes applied to the original draft:
      - `ref('bronze_trade')` -> `source('bronze', 'bronze_trade')`:
        bronze tables are raw sources, not other dbt models.
      - `{{ trim_or_null(t_s_symb) }}` -> `{{ trim_or_null('t_s_symb') }}`:
        the macro takes a column-name string, not a bare identifier.
      - Missing commas after _batch_id/_source_file/_loaded_at (syntax
        error in the original -- would not have compiled).
      - `brinze_trade` typo -> `bronze_trade_src`.
      - `select *, <window fn>` in the final CTE mixed helper columns
        (cdc_flag, prev_status_id, rn, etc.) into the output -- replaced
        with an explicit column list, matching every other silver model.

    No forward-fill in this model, unlike account/customer -- CONFIRMED
    (not assumed): each bronze_trade event sends its full attribute
    payload, not a sparse/partial one. Unlike account/customer's XML
    source, there's no actiontype-driven gap to fill here.

    TRACKED (drives a new version): status_id -- the actual lifecycle
    signal this model exists to capture.
    CARRIED-ONLY (present on every row, doesn't alone trigger a new
    version): trade_type_id, is_cash, symbol, quantity, bid_price,
    customer_account_id, execution_name, trade_price, charge, commission,
    tax. Delegated column-selection decision -- these mostly get set once
    (e.g. at execution) rather than revised repeatedly, so tracking them
    wasn't judged necessary; adjust if wrong.
-#}

with bronze_trade_src as (
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

-- Defense-in-depth only: drops literal duplicate ingestion of the same
-- event. Every distinct event/status-change survives this step.
deduped as (
    {{ dedup_latest('bronze_trade_src', '_row_hash', '_batch_id desc, trade_timestamp desc, _cdc_dsn desc, _loaded_at desc') }}
),

-- Only TRACKED (status_id) drives a new version -- compared against the
-- immediately preceding EVENT, not the preceding batch. No forward-fill
-- step here (confirmed each event sends a full payload) -- straight
-- from `deduped`.
changed_only as (
    select
        *,
        lag(status_id) over (partition by trade_id order by _batch_id, trade_timestamp, _cdc_dsn, _loaded_at) as prev_status_id
    from deduped
),

versions as (
    select *
    from changed_only
    where prev_status_id is null   -- first version ever seen for this trade
       or status_id is distinct from prev_status_id
),

final as (
    select
        *,
        lead(trade_timestamp) over (partition by trade_id order by _batch_id, trade_timestamp, _cdc_dsn, _loaded_at) as next_event_ts,
        row_number() over (partition by trade_id order by _batch_id desc, trade_timestamp desc, _cdc_dsn desc, _loaded_at desc) = 1 as is_current
    from versions
)

select
    {{ silver_surrogate_key(['trade_id', '_batch_id', 'trade_timestamp', '_cdc_dsn']) }} as trade_version_sk,
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
    trade_timestamp                                as valid_from_datetime,
    coalesce(next_event_ts, timestamp '9999-12-31') as valid_to_datetime,
    is_current,
    _batch_id
from final