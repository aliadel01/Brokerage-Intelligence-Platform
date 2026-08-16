with final as (
    select
        {{ gen_surrogate_key(['trade_type_id']) }} as trade_type_sk,
        trade_type_id                          as trade_type_code,
        coalesce(trade_type_name, 'Unknown')   as trade_type_name,
        is_sell_flag,
        is_market_flag                          as is_market_order_flag
    from {{ ref('silver_trade_type') }}
),

unknown as (
    select
        '-1'                       as trade_type_sk,
        'UNK'                    as trade_type_code,
        'Unknown'                as trade_type_name,
        cast(null as boolean)    as is_sell_flag,
        cast(null as boolean)    as is_market_order_flag
)

select * from final
union all
select * from unknown