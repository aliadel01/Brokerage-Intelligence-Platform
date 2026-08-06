{#-
    Same "latest per business key" collapse as dim_company -- silver
    keeps every posting; gold needs exactly one row per symbol
    (UNIQUE(symbol) in the DDL).

    company_sk resolved by matching on company_cik when populated,
    else company_name -- mirrors bronze's own _resolve_co_name_or_cik
    logic (see silver_finwire_security.sql comment). Depends on
    dim_company, not on re-deriving "latest per cik" a second time.
-#}

with latest_security as (
    {{ dedup_latest(ref('silver_finwire_security'), 'security_symbol', 'posting_ts desc') }}
),

company as (
    select company_sk, company_cik, company_name from {{ ref('dim_company') }}
)

select
    {{ surrogate_key(['latest_security.security_symbol']) }} as security_sk,
    latest_security.security_symbol   as symbol,
    latest_security.issue_type,
    latest_security.status            as security_status,
    latest_security.security_name,
    latest_security.exchange_id,
    latest_security.shares_outstanding,
    latest_security.first_trade_date,
    latest_security.first_trade_exchange_date,
    latest_security.dividend,
    company.company_sk
from latest_security
left join company
    on (
        latest_security.company_cik is not null
        and latest_security.company_cik = company.company_cik
    )
    or (
        latest_security.company_cik is null
        and latest_security.company_name = company.company_name
    )