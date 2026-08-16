{{ config(
    materialized='table',
    post_hook=apply_pii_masking(
      string_cols=['last_name','first_name','middle_name','gender','address_line1','address_line2','postal_code','city','state_province','country','primary_email','alternate_email','tax_id','phone1_country_code','phone1_area_code','phone1_number','phone1_extension','phone2_country_code','phone2_area_code','phone2_number','phone2_extension','phone3_country_code','phone3_area_code','phone3_number','phone3_extension'],
      date_cols=['date_of_birth']
    )
) }}
{#-
    SCD Type 2, unifying two bronze sources for the same entity:
      - bronze_customer: real flat-file CDC, Batch2/3 onward.
      - bronze_mgmt_customer: CustomerMgmt.xml, Batch1/Historical Load only.

    GRAIN = one row per customer_id per day's LAST event,
    kept ONLY if it differs (on tracked columns) from the previous KEPT
    day's version. 
    
    Sequence matters: forward-fill runs on EVERY individual event first
    (full ordering chain), THEN the per-batch collapse picks the last
    event of that batch -- collapsing first would discard values an
    earlier same-batch event contributed before they got forward-filled.

    Other decisions (full trail in silver_account_customer_decisions.md):
      - actiontype -> cdc_flag: NEW -> I, UPDCUST/INACT -> U.
      - actiontype -> status_id: NEW -> 'ACTV', UPDCUST -> NULL
        (forward-filled), INACT -> 'INAC' (TPC-DI spec: "An existing
        customer has become inactive").
      - CLOSEACCT/ADDACCT/UPDACCT rows carry zero customer attributes --
        excluded, account-scoped events in the same flattened stream.
      - valid_from_date/valid_to_date use bronze_batch_control.asofdate.
      - TRACKED: status_id, last_name, first_name, tier, address_line1,
        city, state_province, country, primary_email. CARRIED-ONLY:
        middle_name, gender, date_of_birth, address_line2, postal_code,
        alternate_email, tax_id, local_tax_rate_id, national_tax_rate_id,
        all 3 phone numbers (delegated column-selection decision).
-#}

with bronze_flat as (
    select
        c_id                                                as customer_id,
        {{ trim_or_null('c_l_name') }}                     as last_name,
        {{ trim_or_null('c_f_name') }}                     as first_name,
        {{ trim_or_null('c_m_name', uppercase=true) }}     as middle_name,
        {{ trim_or_null('c_gndr', uppercase=true) }}       as gender,
        c_tier                                               as tier,
        c_dob                                                as date_of_birth,
        {{ trim_or_null('c_adline1') }}                    as address_line1,
        {{ trim_or_null('c_adline2') }}                    as address_line2,
        {{ trim_or_null('c_zipcode') }}                    as postal_code,
        {{ trim_or_null('c_city') }}                        as city,
        {{ trim_or_null('c_state_prov') }}                 as state_province,
        {{ trim_or_null('c_ctry') }}                        as country,
        {{ trim_or_null('c_prim_email') }}                 as primary_email,
        {{ trim_or_null('c_alt_email') }}                  as alternate_email,
        {{ trim_or_null('c_tax_id') }}                     as tax_id,
        {{ trim_or_null('c_lcl_tx_id', uppercase=true) }}  as local_tax_rate_id,
        {{ trim_or_null('c_nat_tx_id', uppercase=true) }}  as national_tax_rate_id,
        {{ trim_or_null('c_ctry_1') }}                     as phone1_country_code,
        {{ trim_or_null('c_area_1') }}                     as phone1_area_code,
        {{ trim_or_null('c_local_1') }}                    as phone1_number,
        {{ trim_or_null('c_ext_1') }}                      as phone1_extension,
        {{ trim_or_null('c_ctry_2') }}                     as phone2_country_code,
        {{ trim_or_null('c_area_2') }}                     as phone2_area_code,
        {{ trim_or_null('c_local_2') }}                    as phone2_number,
        {{ trim_or_null('c_ext_2') }}                      as phone2_extension,
        {{ trim_or_null('c_ctry_3') }}                     as phone3_country_code,
        {{ trim_or_null('c_area_3') }}                     as phone3_area_code,
        {{ trim_or_null('c_local_3') }}                    as phone3_number,
        {{ trim_or_null('c_ext_3') }}                      as phone3_extension,
        {{ trim_or_null('c_st_id', uppercase=true) }}      as status_id,
        {{ trim_or_null('_cdc_flag', uppercase=true) }}    as cdc_flag,
        _cdc_dsn,
        cast(null as timestamp_ntz)                          as action_ts,
        _batch_id,
        _row_hash,
        _loaded_at,
        'bronze_customer'                                    as _source_table
    from {{ source('bronze', 'bronze_customer') }}
    where c_id is not null
),

bronze_xml as (
    select
        c_id                                                as customer_id,
        {{ trim_or_null('c_l_name') }}                     as last_name,
        {{ trim_or_null('c_f_name') }}                     as first_name,
        {{ trim_or_null('c_m_name', uppercase=true) }}     as middle_name,
        {{ trim_or_null('c_gndr', uppercase=true) }}       as gender,
        c_tier                                               as tier,
        c_dob                                                as date_of_birth,
        {{ trim_or_null('c_adline1') }}                    as address_line1,
        {{ trim_or_null('c_adline2') }}                    as address_line2,
        {{ trim_or_null('c_zipcode') }}                    as postal_code,
        {{ trim_or_null('c_city') }}                        as city,
        {{ trim_or_null('c_state_prov') }}                 as state_province,
        {{ trim_or_null('c_ctry') }}                        as country,
        {{ trim_or_null('c_prim_email') }}                 as primary_email,
        {{ trim_or_null('c_alt_email') }}                  as alternate_email,
        {{ trim_or_null('c_tax_id') }}                     as tax_id,
        {{ trim_or_null('c_lcl_tx_id', uppercase=true) }}  as local_tax_rate_id,
        {{ trim_or_null('c_nat_tx_id', uppercase=true) }}  as national_tax_rate_id,
        {{ trim_or_null('c_ctry_1') }}                     as phone1_country_code,
        {{ trim_or_null('c_area_1') }}                     as phone1_area_code,
        {{ trim_or_null('c_local_1') }}                    as phone1_number,
        {{ trim_or_null('c_ext_1') }}                      as phone1_extension,
        {{ trim_or_null('c_ctry_2') }}                     as phone2_country_code,
        {{ trim_or_null('c_area_2') }}                     as phone2_area_code,
        {{ trim_or_null('c_local_2') }}                    as phone2_number,
        {{ trim_or_null('c_ext_2') }}                      as phone2_extension,
        {{ trim_or_null('c_ctry_3') }}                     as phone3_country_code,
        {{ trim_or_null('c_area_3') }}                     as phone3_area_code,
        {{ trim_or_null('c_local_3') }}                    as phone3_number,
        {{ trim_or_null('c_ext_3') }}                      as phone3_extension,
        case {{ trim_or_null('actiontype', uppercase=true) }}
            when 'NEW'   then 'ACTV'
            when 'INACT' then 'INAC'
            else null   -- UPDCUST: no status signal; forward-fill
        end                                                   as status_id,
        case {{ trim_or_null('actiontype', uppercase=true) }}
            when 'NEW' then 'I'
            else 'U'
        end                                                   as cdc_flag,
        0                                                     as _cdc_dsn,
        actionts                                              as action_ts,
        _batch_id,
        _row_hash,
        _loaded_at,
        'bronze_mgmt_customer'                                as _source_table
    from {{ source('bronze', 'bronze_mgmt_customer') }}
    where c_id is not null
      and {{ trim_or_null('actiontype', uppercase=true) }} in ('NEW', 'UPDCUST', 'INACT')
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
        customer_id,
        last_value(last_name)             ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as last_name,
        last_value(first_name)            ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as first_name,
        last_value(middle_name)           ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as middle_name,
        last_value(gender)                ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as gender,
        last_value(tier)                  ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as tier,
        last_value(date_of_birth)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as date_of_birth,
        last_value(address_line1)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as address_line1,
        last_value(address_line2)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as address_line2,
        last_value(postal_code)           ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as postal_code,
        last_value(city)                  ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as city,
        last_value(state_province)        ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as state_province,
        last_value(country)               ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as country,
        last_value(primary_email)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as primary_email,
        last_value(alternate_email)       ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as alternate_email,
        last_value(tax_id)                ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as tax_id,
        last_value(local_tax_rate_id)     ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as local_tax_rate_id,
        last_value(national_tax_rate_id)  ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as national_tax_rate_id,
        last_value(phone1_country_code)   ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone1_country_code,
        last_value(phone1_area_code)      ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone1_area_code,
        last_value(phone1_number)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone1_number,
        last_value(phone1_extension)      ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone1_extension,
        last_value(phone2_country_code)   ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone2_country_code,
        last_value(phone2_area_code)      ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone2_area_code,
        last_value(phone2_number)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone2_number,
        last_value(phone2_extension)      ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone2_extension,
        last_value(phone3_country_code)   ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone3_country_code,
        last_value(phone3_area_code)      ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone3_area_code,
        last_value(phone3_number)         ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone3_number,
        last_value(phone3_extension)      ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as phone3_extension,
        last_value(status_id)             ignore nulls over (partition by customer_id order by _batch_id, action_ts, _cdc_dsn, _loaded_at rows unbounded preceding) as status_id,
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
    qualify row_number() over (partition by customer_id, _batch_id, action_ts::DATE  order by action_ts desc, _cdc_dsn desc, _loaded_at desc) = 1
),

-- Compare each day's kept version against the PREVIOUS day's version
-- on tracked columns only.
changed_only as (
    select
        *,
        lag(status_id)      over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_status_id,
        lag(last_name)      over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_last_name,
        lag(first_name)     over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_first_name,
        lag(tier)           over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_tier,
        lag(address_line1)  over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_address_line1,
        lag(city)           over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_city,
        lag(state_province) over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_state_province,
        lag(country)        over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_country,
        lag(primary_email)  over (partition by customer_id order by _batch_id , action_ts, _cdc_dsn, _loaded_at) as prev_primary_email
    from one_row_per_day
),

versions as (
    select *
    from changed_only
    where prev_status_id is null   -- first version ever seen for this customer
       or (status_id, last_name, first_name, tier, address_line1, city, state_province, country, primary_email)
          is distinct from
          (prev_status_id, prev_last_name, prev_first_name, prev_tier, prev_address_line1, prev_city, prev_state_province, prev_country, prev_primary_email)
),

with_dates as (
    select
        v.*,
    CASE 
        WHEN v._source_table = 'bronze_mgmt_customer' THEN  cast(v.action_ts as date)
        ELSE bc.asofdate
    END as valid_from_date
        from versions v
    left join {{ source('bronze', 'bronze_batch_control') }} bc
        on v._batch_id = bc._batch_id
),


final as (
    select
        *,
        lead(valid_from_date) over (partition by customer_id order by _batch_id) as next_valid_from_date,
        row_number() over (partition by customer_id order by _batch_id desc) = 1 as is_current
    from with_dates
)

select
    {{ gen_surrogate_key(['customer_id', '_batch_id', 'valid_from_date']) }} as customer_version_sk,
    customer_id,
    last_name,
    first_name,
    middle_name,
    gender,
    tier,
    date_of_birth,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    primary_email,
    alternate_email,
    tax_id,
    local_tax_rate_id,
    national_tax_rate_id,
    phone1_country_code, phone1_area_code, phone1_number, phone1_extension,
    phone2_country_code, phone2_area_code, phone2_number, phone2_extension,
    phone3_country_code, phone3_area_code, phone3_number, phone3_extension,
    status_id,
    cdc_flag,
    valid_from_date,
    coalesce(dateadd(day, -1, next_valid_from_date), date '9999-12-31') as valid_to_date,
    is_current,
    action_ts,
    _cdc_dsn,
    _batch_id,
    _loaded_at,
    _source_table
from final