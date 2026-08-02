{#-
    SCD Type 1 Strategy (Latest State / Overwrite):
    This model implements Type 1 Slowing Changing Dimension for the Prospect entity.
    Unlike periodic snapshots, we collapse across ALL batches to keep only the 
    single latest record per agency_id.

    Dedup & Collapse Logic:
    1. Partitioning by 'agency_id' forces all historical iterations of a prospect 
       into a single group.
    2. Ordering by '_batch_id desc, _loaded_at desc' ensures that newer batch 
       dumps overwrite older historical states.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_prospect') }}
),

cleaned as (
    select
        {{ trim_or_null('agencyid') }}                    as agency_id,
        {{ trim_or_null('lastname') }}                    as last_name,
        {{ trim_or_null('firstname') }}                   as first_name,
        {{ trim_or_null('middleinitial', uppercase=true) }} as middle_initial,
        {{ trim_or_null('gender', uppercase=true) }}      as gender,
        {{ trim_or_null('addressline1') }}                as address_line1,
        {{ trim_or_null('addressline2') }}                as address_line2,
        {{ trim_or_null('postalcode') }}                  as postal_code,
        {{ trim_or_null('city') }}                        as city,
        {{ trim_or_null('state', uppercase=true) }}       as state,
        {{ trim_or_null('country') }}                     as country,
        {{ trim_or_null('phone') }}                        as phone,
        income,
        numbercars                                          as number_cars,
        numberchildren                                      as number_children,
        {{ trim_or_null('maritalstatus', uppercase=true) }} as marital_status,
        age,
        creditrating                                        as credit_rating,
        {{ trim_or_null('ownorrentflag', uppercase=true) }} as own_or_rent_flag,
        {{ trim_or_null('employer') }}                    as employer,
        numbercreditcards                                   as number_credit_cards,
        networth                                            as net_worth,

        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where agencyid is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'agency_id', '_batch_id desc, _loaded_at desc') }}
)

select * from deduped