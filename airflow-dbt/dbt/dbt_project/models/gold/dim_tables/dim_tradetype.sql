select
    {{ surrogate_key(['trade_type_id']) }} as trade_type_sk,
    trade_type_id     as trade_type_code,
    trade_type_name,
    is_sell_flag,
    is_market_flag    as is_market_order_flag
from {{ ref('silver_trade_type') }}