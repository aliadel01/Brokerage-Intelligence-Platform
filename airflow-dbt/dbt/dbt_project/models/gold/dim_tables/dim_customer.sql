{#-
    Grain: One row per customer version (SCD Type 2).

    SCD2 pass-through model: silver_customer already handles SCD Type 2 logic,
    generating customer_version_sk and maintaining valid_from_date/valid_to_date
    history.

    Gold level transformations:
    - Passes through customer_version_sk as customer_sk to guarantee foreign key
      stability across downstream models (e.g., dim_account).
    - Denormalizes local and national tax rates via two separate joins to
      silver_tax_rate (per gold.md ADR-003).
    - Formats business attribute aliases and converts boundary dates.
-#}

with customer as (
    select * from {{ ref('silver_customer') }}
),

tax_rate as (
    select tax_rate_id, tax_rate_name, tax_rate from {{ ref('silver_tax_rate') }}
)

select
    customer.customer_version_sk as customer_sk,
    customer.customer_id,
    customer.tax_id,
    customer.status_id                                as customer_status,
    customer.last_name,
    customer.first_name,
    customer.middle_name,
    customer.gender,
    customer.tier                                     as customer_tier,
    customer.date_of_birth,
    customer.address_line1,
    customer.address_line2,
    customer.postal_code,
    customer.city,
    customer.state_province,
    customer.country,
    customer.phone1_country_code,
    customer.phone1_area_code,
    customer.phone1_number                            as phone1_local_number,
    customer.phone1_extension,
    customer.phone2_country_code,
    customer.phone2_area_code,
    customer.phone2_number                            as phone2_local_number,
    customer.phone2_extension,
    customer.phone3_country_code,
    customer.phone3_area_code,
    customer.phone3_number                            as phone3_local_number,
    customer.phone3_extension,
    customer.primary_email,
    customer.alternate_email,
    customer.valid_from_date                          as effective_start_date,
    nullif(customer.valid_to_date, date '9999-12-31') as effective_end_date,
    customer.is_current,
    customer.cdc_flag,
    customer.local_tax_rate_id                        as local_tax_rate_code,
    local_tax.tax_rate_name                           as local_tax_rate_name,
    local_tax.tax_rate                                as local_tax_rate_pct,
    customer.national_tax_rate_id                     as national_tax_rate_code,
    national_tax.tax_rate_name                        as national_tax_rate_name,
    national_tax.tax_rate                             as national_tax_rate_pct
from customer
left join tax_rate as local_tax
    on customer.local_tax_rate_id = local_tax.tax_rate_id
left join tax_rate as national_tax
    on customer.national_tax_rate_id = national_tax.tax_rate_id