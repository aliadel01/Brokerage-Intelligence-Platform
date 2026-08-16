{#-
    year/quarter kept as degenerate dims per gold.md ADR-009.
    fiscal_date_sk from qtr_start_date; posting_date_sk from
    posting_date (not posting_ts).
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
),

resolved as (
    select
        coalesce(company.company_sk, '-1')         as company_sk,
        coalesce(fiscal_date.date_sk, '-1')        as fiscal_date_sk,
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
        coalesce(posting_date_dim.date_sk, '-1')   as posting_date_sk
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
)

select
    {{ gen_surrogate_key(['company_sk', 'fiscal_date_sk']) }} as company_financials_sk,
    *
from resolved