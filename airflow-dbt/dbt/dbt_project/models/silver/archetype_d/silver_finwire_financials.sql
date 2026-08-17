{{ config(
    materialized='table',
    post_hook=apply_classification_tags(
        relation='silver.silver_finwire_financials',
      tags={
        'posting_ts': 'internal',
        'year': 'public',
        'quarter': 'public',
        'qtr_start_date': 'public',
        'posting_date': 'public',
        'revenue': 'public',
        'earnings': 'public',
        'eps': 'public',
        'diluted_eps': 'public',
        'margin': 'public',
        'inventory': 'public',
        'assets': 'public',
        'liabilities': 'public',
        'shares_outstanding': 'public',
        'diluted_shares_outstanding': 'public',
        'company_name': 'public',
        'company_cik': 'public',
        '_batch_id': 'internal',
        '_row_hash': 'internal',
        '_loaded_at': 'internal'
      }
    )
) }}
{#-
    Archetype D (parsed): Batch1 only, append-only-by-PTS, no CDC columns
    in the source file at all. Natural key uses coalesce(cik, name) since
    CoNameOrCIK is polymorphic -- a given company is identified by
    whichever of the two bronze already resolved it to.
-#}

with source as (
    select * from {{ source('bronze', 'bronze_finwire_fin') }}
),

cleaned as (
    select
        pts                                                  as posting_ts,
        year,
        quarter,
        qtrstartdate                                          as qtr_start_date,
        postingdate                                           as posting_date,
        revenue,
        earnings,
        eps,
        dilutedeps                                             as diluted_eps,
        margin,
        inventory,
        assets,
        liabilities,
        shout                                                  as shares_outstanding,
        dilutedshout                                           as diluted_shares_outstanding,
        {{ trim_or_null('coname') }}                           as company_name,
        {{ trim_or_null('cocik') }}                            as company_cik,

        _batch_id,
        _row_hash,
        _loaded_at
    from source
    where pts is not null
),

deduped as (
    {{ dedup_latest('cleaned', 'coalesce(company_cik, company_name), year, quarter', '_loaded_at desc') }}
)

select * from deduped
