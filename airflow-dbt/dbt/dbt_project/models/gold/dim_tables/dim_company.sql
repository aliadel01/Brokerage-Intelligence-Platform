{#-
    Grain: One row per company (Latest version).

    silver_finwire_company is append-only-by-posting_ts (Archetype D) --
    more than one row per cik can exist. dim_company needs exactly one
    (UNIQUE(company_cik) in the DDL), so we pick the latest posting per
    cik here, in gold, not in silver (silver's job was cleaning/dedup of
    true duplicates, not collapsing a legitimate append-only history to
    latest-state -- that's a gold-layer dimensional decision).

    industry_name/sector_id resolved via join to silver_industry, per
    gold.md ADR-002 -- not present directly in silver_finwire_company.
-#}

with latest_company as (
    {{ dedup_latest(ref('silver_finwire_company'), 'cik', 'posting_ts desc') }}
),

industry as (
    select * from {{ ref('silver_industry') }}
)

select
    {{ surrogate_key(['latest_company.cik']) }} as company_sk,
    latest_company.cik              as company_cik,
    latest_company.company_name,
    latest_company.status           as company_status,
    latest_company.sp_rating,
    latest_company.founding_date,
    latest_company.address_line1,
    latest_company.address_line2,
    latest_company.postal_code,
    latest_company.city,
    latest_company.state_province,
    latest_company.country,
    latest_company.ceo_name,
    latest_company.description      as company_description,
    industry.industry_id            as industry_code,
    industry.industry_name,
    industry.sector_id
from latest_company
left join industry
    on latest_company.industry_id = industry.industry_id