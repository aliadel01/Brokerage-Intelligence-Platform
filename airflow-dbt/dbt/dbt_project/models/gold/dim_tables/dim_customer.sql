{#-
    Grain: One row per customer version (SCD Type 2), plus one Unknown
    member row (customer_sk = -1).

    Categorical/attribute columns coalesced to 'Unknown' when NULL.
    Free-text/identifier columns (names, address lines, tax_id, DOB,
    phones, emails) left NULL -- not a bucketable category.
-#}

with customer as (
    select * from {{ ref('silver_customer') }}
),

tax_rate as (
    select tax_rate_id, tax_rate_name, tax_rate from {{ ref('silver_tax_rate') }}
),

final as (
    select
        customer.customer_version_sk as customer_sk,
        customer.customer_id,
        customer.tax_id,
        coalesce(customer.status_id, 'Unknown')            as customer_status,
        customer.last_name,
        customer.first_name,
        customer.middle_name,
        coalesce(customer.gender, 'Unknown')               as gender,
        coalesce(customer.tier, 'Unknown')                 as customer_tier,
        customer.date_of_birth,
        customer.address_line1,
        customer.address_line2,
        customer.postal_code,
        customer.city,
        coalesce(customer.state_province, 'Unknown')       as state_province,
        coalesce(customer.country, 'Unknown')              as country,
        customer.phone1_country_code,
        customer.phone1_area_code,
        customer.phone1_number                             as phone1_local_number,
        customer.phone1_extension,
        customer.phone2_country_code,
        customer.phone2_area_code,
        customer.phone2_number                             as phone2_local_number,
        customer.phone2_extension,
        customer.phone3_country_code,
        customer.phone3_area_code,
        customer.phone3_number                             as phone3_local_number,
        customer.phone3_extension,
        customer.primary_email,
        customer.alternate_email,
        customer.valid_from_date                           as effective_start_date,
        nullif(customer.valid_to_date, date '9999-12-31')  as effective_end_date,
        customer.is_current,
        customer.cdc_flag,
        customer.local_tax_rate_id                          as local_tax_rate_code,
        coalesce(local_tax.tax_rate_name, 'Unknown')         as local_tax_rate_name,
        local_tax.tax_rate                                  as local_tax_rate_pct,
        customer.national_tax_rate_id                       as national_tax_rate_code,
        coalesce(national_tax.tax_rate_name, 'Unknown')      as national_tax_rate_name,
        national_tax.tax_rate                               as national_tax_rate_pct
    from customer
    left join tax_rate as local_tax
        on customer.local_tax_rate_id = local_tax.tax_rate_id
    left join tax_rate as national_tax
        on customer.national_tax_rate_id = national_tax.tax_rate_id
),

unknown as (
    select
        -1                    as customer_sk,
        'UNKNOWN'             as customer_id,
        cast(null as varchar) as tax_id,
        'Unknown'             as customer_status,
        'Unknown'             as last_name,
        'Unknown'             as first_name,
        cast(null as varchar) as middle_name,
        'Unknown'             as gender,
        'Unknown'             as customer_tier,
        cast(null as date)    as date_of_birth,
        cast(null as varchar) as address_line1,
        cast(null as varchar) as address_line2,
        cast(null as varchar) as postal_code,
        cast(null as varchar) as city,
        'Unknown'             as state_province,
        'Unknown'             as country,
        cast(null as varchar) as phone1_country_code,
        cast(null as varchar) as phone1_area_code,
        cast(null as varchar) as phone1_local_number,
        cast(null as varchar) as phone1_extension,
        cast(null as varchar) as phone2_country_code,
        cast(null as varchar) as phone2_area_code,
        cast(null as varchar) as phone2_local_number,
        cast(null as varchar) as phone2_extension,
        cast(null as varchar) as phone3_country_code,
        cast(null as varchar) as phone3_area_code,
        cast(null as varchar) as phone3_local_number,
        cast(null as varchar) as phone3_extension,
        cast(null as varchar) as primary_email,
        cast(null as varchar) as alternate_email,
        date '1900-01-01'     as effective_start_date,
        cast(null as date)    as effective_end_date,
        false                 as is_current,
        'U'                   as cdc_flag,
        cast(null as varchar) as local_tax_rate_code,
        'Unknown'             as local_tax_rate_name,
        cast(null as numeric) as local_tax_rate_pct,
        cast(null as varchar) as national_tax_rate_code,
        'Unknown'             as national_tax_rate_name,
        cast(null as numeric) as national_tax_rate_pct
)

select * from final
union all
select * from unknown