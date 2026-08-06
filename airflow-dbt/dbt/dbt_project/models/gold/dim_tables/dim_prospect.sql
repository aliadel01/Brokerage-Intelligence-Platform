{#-
    customer_sk is left NULL for every row -- the Prospect-to-Customer
    matching rule (TPC-DI spec Clause 4.5.x) is still an open question
    (gold.md Open Questions #1 / 04_silver.md Open Questions #1). Column
    is nullable per gold.md ADR-004. Once the matching rule is
    confirmed, this model needs a join added to populate it.
-#}

select
    {{ surrogate_key(['agency_id']) }} as prospect_sk,
    cast(null as bigint)  as customer_sk,
    agency_id,
    last_name,
    first_name,
    middle_initial,
    gender,
    address_line1,
    address_line2,
    postal_code,
    city,
    state,
    country,
    phone,
    income                as annual_income,
    number_cars           as number_of_cars,
    number_children       as number_of_children,
    marital_status,
    age,
    credit_rating,
    own_or_rent_flag,
    employer              as employer_name,
    number_credit_cards   as number_of_credit_cards,
    net_worth
from {{ ref('silver_prospect') }}