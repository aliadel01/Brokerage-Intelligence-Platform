{#-
    SCD Type 2, unifying two bronze sources for the same entity:
      - bronze_account: real flat-file CDC, Batch2/3 onward.
      - bronze_mgmt_account: CustomerMgmt.xml, Batch1/Historical Load only.

    GRAIN = one row per account_id per day's LAST event, 
    kept ONLY if it differs (on tracked columns) from the previous KEPT day's version. 

    Sequence matters here and was a real bug in an earlier draft:
    forward-fill must run on EVERY individual event first (full ordering
    chain), THEN the per-batch collapse picks the last event of that
    batch -- collapsing first would have discarded values an earlier
    event in the same batch contributed before they got forward-filled.

    Other decisions (full trail in silver/account_customer_decisions.md):
      - actiontype -> cdc_flag: NEW -> I, ADDACCT/UPDACCT/CLOSEACCT -> U.
      - actiontype -> status_id: NEW/ADDACCT -> 'ACTV', UPDACCT -> NULL
        (forward-filled), CLOSEACCT -> 'INAC' (TPC-DI spec).
      - valid_from_date/valid_to_date use bronze_batch_control.asofdate.
      - TRACKED: status_id, account_name. CARRIED-ONLY: broker_id,
        tax_status (delegated column-selection decision).
      - action_ts NULL for flat-CDC rows, populated for XML rows -- safe
        under Snowflake's NULL-sort default only because a batch never
        mixes both sources.
-#}

with bronze_flat as (
    select
        ca_id                                             as account_id,
        ca_b_id                                            as broker_id,
        ca_c_id                                            as customer_id,
        {{ trim_or_null('ca_name') }}                     as account_name,
        ca_tax_st                                          as tax_status,
        {{ trim_or_null('ca_st_id', uppercase=true) }}    as status_id,
        {{ trim_or_null('_cdc_flag', uppercase=true) }}   as cdc_flag,
        _cdc_dsn,
        cast(null as timestamp_ntz)                        as action_ts,
        _batch_id,
        _row_hash,
        _loaded_at,
        'bronze_account'                                    as _source_table
    from {{ source('bronze', 'bronze_account') }}
    where ca_id is not null
),

bronze_xml as (
    select
        ca_id                                             as account_id,
        ca_b_id                                            as broker_id,
        c_id                                                as customer_id,
        {{ trim_or_null('ca_name') }}                     as account_name,
        ca_tax_st                                          as tax_status,
        case {{ trim_or_null('actiontype', uppercase=true) }}
            when 'NEW'       then 'ACTV'
            when 'ADDACCT'   then 'ACTV'
            when 'CLOSEACCT' then 'INAC'
            else null   -- UPDACCT: no status signal; forward-fill
        end                                                  as status_id,
        case {{ trim_or_null('actiontype', uppercase=true) }}
            when 'NEW' then 'I'
            else 'U'
        end                                                  as cdc_flag,
        0                                                    as _cdc_dsn,
        actionts                                             as action_ts,
        _batch_id,
        _row_hash,
        _loaded_at,
        'bronze_mgmt_account'                                as _source_table
    from {{ source('bronze', 'bronze_mgmt_account') }}
    where ca_id is not null
),

unioned as (
    select * from bronze_flat
    union all
    select * from bronze_xml
),

deduped as (
    {{ dedup_latest('unioned', '_row_hash', '_batch_id desc, action_ts desc, _cdc_dsn desc, _loaded_at desc') }}
),

-- Forward-fill EVERY individual event first, full ordering chain.
filled as (
    select
        account_id,
        last_value(broker_id)    ignore nulls over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as broker_id,
        last_value(customer_id)  ignore nulls over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as customer_id,
        last_value(account_name) ignore nulls over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as account_name,
        last_value(tax_status)   ignore nulls over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as tax_status,
        last_value(status_id)    ignore nulls over (partition by account_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as status_id,
        cdc_flag,
        action_ts,
        _cdc_dsn,
        _batch_id,
        _loaded_at,
        _source_table
    from deduped
),

one_row_per_day as (
    select *
    from filled
    qualify row_number() over (partition by account_id, _batch_id, action_ts::DATE  order by action_ts desc, _cdc_dsn desc, _loaded_at desc) = 1
),

-- Compare each day's kept version against the PREVIOUS day's version
-- on tracked columns only.
changed_only as (
    select
        *,
        lag(status_id)    over (partition by account_id order by _batch_id) as prev_status_id,
        lag(account_name) over (partition by account_id order by _batch_id) as prev_account_name
    from one_row_per_day
),

versions as (
    select *
    from changed_only
    where prev_status_id is null   -- first version ever seen for this account
       or (status_id, account_name) is distinct from (prev_status_id, prev_account_name)
),

with_dates as (
    select
        v.*,
        CASE 
            WHEN v._source_table = 'bronze_mgmt_account' THEN  cast(v.action_ts as date)
        ELSE bc.asofdate
    END as valid_from_date
    from versions v
    left join {{ source('bronze', 'bronze_batch_control') }} bc
        on v._batch_id = bc._batch_id
),

final as (
    select
        *,
        lead(valid_from_date) over (partition by account_id order by _batch_id) as next_valid_from_date,
        row_number() over (partition by account_id order by _batch_id desc) = 1 as is_current
    from with_dates
)

select
    {{ surrogate_key(['account_id', '_batch_id', 'valid_from_date']) }} as account_version_sk,
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    cdc_flag,
    valid_from_date,
    coalesce(dateadd(day, -1, next_valid_from_date), date '9999-12-31') as valid_to_date,
    is_current,
    _batch_id,
    _source_table
from final