{#-
    Archetype D (parsed): Batch1 only, append-only-by-PTS, no CDC columns
    in the source file at all. Dedup by (cik, posting_ts) only removes
    true duplicate ingestion of the same event, not different companies.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_finwire_cmp') }}
),

cleaned as (
    select
        pts                                                as posting_ts,
        {{ trim_or_null('companyname') }}                 as company_name,
        {{ trim_or_null('cik') }}                          as cik,
        {{ trim_or_null('status', uppercase=true) }}      as status,
        {{ trim_or_null('industryid', uppercase=true) }}  as industry_id,
        {{ trim_or_null('sprating', uppercase=true) }}    as sp_rating,
        foundingdate                                        as founding_date,
        {{ trim_or_null('addrline1') }}                    as address_line1,
        {{ trim_or_null('addrline2') }}                    as address_line2,
        {{ trim_or_null('postalcode') }}                   as postal_code,
        {{ trim_or_null('city') }}                         as city,
        {{ trim_or_null('stateprovince') }}                as state_province,
        {{ trim_or_null('country') }}                      as country,
        {{ trim_or_null('ceoname') }}                      as ceo_name,
        {{ trim_or_null('description') }}                  as description,

        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where pts is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'cik, posting_ts', '_loaded_at desc') }}
)

select * from deduped