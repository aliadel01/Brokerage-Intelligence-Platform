{#-
    year/quarter kept as degenerate dimensions per gold.md ADR-009 --
    silver has them, so gold keeps them rather than depending entirely
    on dim_date's own fiscal fields aligning with FINWIRE's values.

    fiscal_date_sk resolved from qtr_start_date (the fiscal period
    itself); posting_date_sk resolved from posting_date, NOT posting_ts
    (the latter is a precise timestamp used only for silver's own dedup
    ordering, not a reporting date).
-#}

with financials as (
    select * from {{ ref('silver_finwire_financials') }}
),

company as (
    select company_sk, company_cik, company_name from {{ ref('dim_company') }}
),

fiscal_date as (
    select date_sk, date_value from {{ ref('dim_date') }}
),

posting_date_dim as (
    select date_sk, date_value from {{ ref('dim_date') }}
)

select
    company.company_sk,
    fiscal_date.date_sk         as fiscal_date_sk,
    financials.year              as fiscal_year,
    financials.quarter           as fiscal_quarter,
    financials.revenue,
    financials.earnings,
    financials.eps                as eps_basic,
    financials.diluted_eps        as eps_diluted,
    financials.margin             as profit_margin,
    financials.inventory,
    financials.assets             as total_assets,
    financials.liabilities        as total_liabilities,
    financials.shares_outstanding,
    financials.diluted_shares_outstanding,
    financials._batch_id          as batch_id,
    posting_date_dim.date_sk      as posting_date_sk
from financials
left join company
    on (
        financials.company_cik is not null
        and financials.company_cik = company.company_cik
    )
    or (
        financials.company_cik is null
        and financials.company_name = company.company_name
    )
left join fiscal_date
    on fiscal_date.date_value = financials.qtr_start_date
left join posting_date_dim
    on posting_date_dim.date_value = financials.posting_date