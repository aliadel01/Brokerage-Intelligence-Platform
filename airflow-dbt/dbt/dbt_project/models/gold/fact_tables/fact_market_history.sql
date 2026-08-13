with market as (
    select * from {{ ref('silver_daily_market') }}
),

security as (
    select security_sk, symbol from {{ ref('dim_security') }}
),

market_date as (
    select date_sk, date_value from {{ ref('dim_date') }}
),

resolved as (
    select
        coalesce(security.security_sk, -1)  as security_sk,
        coalesce(market_date.date_sk, -1)   as market_date_sk,
        market.close_price,
        market.high_price,
        market.low_price,
        market.volume,
        market._cdc_flag       as cdc_flag,
        market._cdc_dsn        as cdc_dsn,
        market._batch_id       as batch_id
    from market
    left join security
        on market.security_symbol = security.symbol
    left join market_date
        on market_date.date_value = market.market_date
)

select
    {{ surrogate_key(['security_sk', 'market_date_sk']) }} as market_history_sk,
    *
from resolved