{{ config(
    materialized='table',
    post_hook=
      apply_pii_masking(
        relation='gold.dim_prospect',
        string_cols=['last_name','first_name','middle_initial','gender','address_line1','address_line2','postal_code','city','state','country','phone','marital_status','own_or_rent_flag','employer_name'],
        numeric_cols=['annual_income','number_of_cars','number_of_children','age','credit_rating','number_of_credit_cards','net_worth']
      )
      +
      apply_classification_tags(
        relation='gold.dim_prospect',
        tags={
          'prospect_sk': 'internal',
          'customer_sk': 'internal',
          'agency_id': 'confidential',
          'last_name': 'restricted_pii',
          'first_name': 'restricted_pii',
          'middle_initial': 'restricted_pii',
          'gender': 'restricted_pii',
          'address_line1': 'restricted_pii',
          'address_line2': 'restricted_pii',
          'postal_code': 'restricted_pii',
          'city': 'restricted_pii',
          'state': 'restricted_pii',
          'country': 'restricted_pii',
          'phone': 'restricted_pii',
          'annual_income': 'restricted_pii',
          'number_of_cars': 'restricted_pii',
          'number_of_children': 'restricted_pii',
          'marital_status': 'restricted_pii',
          'age': 'restricted_pii',
          'credit_rating': 'restricted_pii',
          'own_or_rent_flag': 'restricted_pii',
          'employer_name': 'restricted_pii',
          'number_of_credit_cards': 'restricted_pii',
          'net_worth': 'restricted_pii'
        }
      )
) }}
{#-
    customer_sk left NULL for every real row -- Prospect-to-Customer
    matching rule still open (gold.md Open Questions #1). Unknown
    member row still included for fact-side consistency, though no
    fact currently FKs to dim_prospect directly (ADR-001 outrigger).
-#}

with prospect_final as (
    select
        {{ gen_surrogate_key(['agency_id']) }} as prospect_sk,
        cast(null as varchar)                    as customer_sk,
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
        '-1'                  as prospect_sk,
        cast(null as varchar) as customer_sk,
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
