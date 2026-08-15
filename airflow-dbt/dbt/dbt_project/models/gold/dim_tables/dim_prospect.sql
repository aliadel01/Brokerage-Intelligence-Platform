{#-
    customer_sk left NULL for every real row -- Prospect-to-Customer
    matching rule still open (gold.md Open Questions #1). Unknown
    member row still included for fact-side consistency, though no
    fact currently FKs to dim_prospect directly (ADR-001 outrigger).
-#}

with prospect_final as (
    select
        {{ gen_surrogate_key(['agency_id']) }} as prospect_sk,
        cast(null as bigint)               as customer_sk,
        agency_id,
        last_name,
        first_name,
        middle_initial,
        coalesce(gender, 'Unknown')        as gender,
        address_line1,
        address_line2,
        postal_code,
        city,
        state,
        country,
        phone,
        income                              as annual_income,
        number_cars                         as number_of_cars,
        number_children                     as number_of_children,
        coalesce(marital_status, 'Unknown') as marital_status,
        age,
        credit_rating,
        own_or_rent_flag,
        employer                            as employer_name,
        number_credit_cards                 as number_of_credit_cards,
        net_worth
    from {{ ref('silver_prospect') }}
),

unknown as (
    select
        -1                    as prospect_sk,
        cast(null as bigint)  as customer_sk,
        cast(null as varchar) as agency_id,
        'Unknown'             as last_name,
        'Unknown'             as first_name,
        cast(null as varchar) as middle_initial,
        'Unknown'             as gender,
        cast(null as varchar) as address_line1,
        cast(null as varchar) as address_line2,
        cast(null as varchar) as postal_code,
        cast(null as varchar) as city,
        cast(null as varchar) as state,
        cast(null as varchar) as country,
        cast(null as varchar) as phone,
        cast(null as numeric) as annual_income,
        cast(null as int)     as number_of_cars,
        cast(null as int)     as number_of_children,
        'Unknown'             as marital_status,
        cast(null as int)     as age,
        cast(null as int)     as credit_rating,
        cast(null as varchar) as own_or_rent_flag,
        cast(null as varchar) as employer_name,
        cast(null as int)     as number_of_credit_cards,
        cast(null as numeric) as net_worth
)

select * from prospect_final
union all
select * from unknown