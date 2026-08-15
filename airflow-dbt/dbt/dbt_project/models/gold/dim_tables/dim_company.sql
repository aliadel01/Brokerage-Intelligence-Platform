{#-
    Grain: One row per company (latest version), plus one Unknown
    member row (company_sk = -1).
-#}

with latest_company as (
    {{ dedup_latest(ref('silver_finwire_company'), 'cik', 'posting_ts desc') }}
),

industry as (
    select * from {{ ref('silver_industry') }}
),

final as (
    select
        {{ gen_surrogate_key(['latest_company.cik']) }} as company_sk,
        latest_company.cik                          as company_cik,
        latest_company.company_name,
        coalesce(latest_company.status, 'Unknown')   as company_status,
        coalesce(latest_company.sp_rating, 'Unknown') as sp_rating,
        latest_company.founding_date,
        latest_company.address_line1,
        latest_company.address_line2,
        latest_company.postal_code,
        latest_company.city,
        latest_company.state_province,
        coalesce(latest_company.country, 'Unknown')  as country,
        latest_company.ceo_name,
        latest_company.description                   as company_description,
        industry.industry_id                          as industry_code,
        coalesce(industry.industry_name, 'Unknown')   as industry_name,
        industry.sector_id
    from latest_company
    left join industry
        on latest_company.industry_id = industry.industry_id
),

unknown as (
    select
        -1                    as company_sk,
        cast(null as bigint)  as company_cik,
        'Unknown'             as company_name,
        'Unknown'             as company_status,
        'Unknown'             as sp_rating,
        cast(null as date)    as founding_date,
        cast(null as varchar) as address_line1,
        cast(null as varchar) as address_line2,
        cast(null as varchar) as postal_code,
        cast(null as varchar) as city,
        cast(null as varchar) as state_province,
        'Unknown'             as country,
        cast(null as varchar) as ceo_name,
        cast(null as varchar) as company_description,
        cast(null as varchar) as industry_code,
        'Unknown'             as industry_name,
        cast(null as varchar) as sector_id
)

select * from final
union all
select * from unknown