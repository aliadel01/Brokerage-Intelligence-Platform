{#-
    Grain: One row per symbol (latest version), plus one Unknown
    member row (security_sk = -1, company_sk = -1 pointing to
    dim_company's own Unknown row).
-#}

with latest_security as (
    {{ dedup_latest(ref('silver_finwire_security'), 'security_symbol', 'posting_ts desc') }}
),

company as (
    select company_sk, company_cik, company_name from {{ ref('dim_company') }}
),

final as (
    select
        {{ surrogate_key(['latest_security.security_symbol']) }} as security_sk,
        latest_security.security_symbol               as symbol,
        coalesce(latest_security.issue_type, 'Unknown') as issue_type,
        coalesce(latest_security.status, 'Unknown')    as security_status,
        latest_security.security_name,
        coalesce(latest_security.exchange_id, 'Unknown') as exchange_id,
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
),

unknown as (
    select
        -1                    as security_sk,
        'UNKNOWN'             as symbol,
        'Unknown'             as issue_type,
        'Unknown'             as security_status,
        'Unknown'             as security_name,
        'Unknown'             as exchange_id,
        cast(null as bigint)  as shares_outstanding,
        cast(null as date)    as first_trade_date,
        cast(null as date)    as first_trade_exchange_date,
        cast(null as numeric) as dividend,
        -1                    as company_sk
)

select * from final
union all
select * from unknown