
-- ---------------------------------------------------------------------------
-- Masking policies — applied to restricted_pii columns in silver/gold.
-- role_custodian and ACCOUNTADMIN see real values; every other role
-- (including role_analyst) sees the masked form.
-- ---------------------------------------------------------------------------

CREATE MASKING POLICY IF NOT EXISTS brokerage_dwh.governance.mask_pii_string AS (val VARCHAR)
    RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_CUSTODIAN', 'ACCOUNTADMIN') THEN val
        ELSE '***MASKED***'
    END;

CREATE MASKING POLICY IF NOT EXISTS brokerage_dwh.governance.mask_pii_date AS (val DATE)
    RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_CUSTODIAN', 'ACCOUNTADMIN') THEN val
        ELSE NULL
    END;

CREATE MASKING POLICY IF NOT EXISTS brokerage_dwh.governance.mask_pii_numeric AS (val NUMBER)
    RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_CUSTODIAN', 'ACCOUNTADMIN') THEN val
        ELSE NULL
    END;

-- ---------------------------------------------------------------------------
-- Apply to bronze_customer's restricted_pii columns (once built — placeholder here,
-- ---------------------------------------------------------------------------

USE SCHEMA brokerage_dwh.bronze;

-- bronze_hr
ALTER TABLE bronze_hr MODIFY
  COLUMN employeefirstname SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN employeelastname SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN employeemi SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN employeephone SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

-- bronze_customer
ALTER TABLE bronze_customer MODIFY
  COLUMN c_tax_id SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_l_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_f_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_m_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_gndr SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_adline1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_adline2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_zipcode SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_state_prov SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_prim_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_alt_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_area_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_local_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ext_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_area_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_local_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ext_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_area_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_local_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ext_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE bronze_customer MODIFY
  COLUMN c_dob SET MASKING POLICY brokerage_dwh.governance.mask_pii_date;

-- bronze_trade
ALTER TABLE bronze_trade MODIFY
  COLUMN t_exec_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

-- bronze_prospect
ALTER TABLE bronze_prospect MODIFY
  COLUMN lastname SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN firstname SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middleinitial SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN addressline1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN addressline2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN postalcode SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN state SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN country SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN gender SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

-- bronze_mgmt_customer (نفس PII surface بتاع bronze_customer)
ALTER TABLE bronze_mgmt_customer MODIFY
  COLUMN c_tax_id SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_l_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_f_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_m_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_gndr SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_adline1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_adline2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_zipcode SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_state_prov SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_prim_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_alt_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_area_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_local_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ext_1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_area_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_local_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ext_2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ctry_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_area_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_local_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN c_ext_3 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE bronze_mgmt_customer MODIFY
  COLUMN c_dob SET MASKING POLICY brokerage_dwh.governance.mask_pii_date;

-- ---------------------------------------------------------------------------
-- Apply to silver_customer's restricted_pii columns
-- (adjust column list to match your actual silver_customer schema)
-- ---------------------------------------------------------------------------
USE SCHEMA brokerage_dwh.silver;

ALTER TABLE brokerage_dwh.silver.silver_hr MODIFY
  COLUMN first_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN last_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middle_initial SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.silver.silver_prospect MODIFY
  COLUMN last_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN first_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middle_initial SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN gender SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN postal_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN state SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN country SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN marital_status SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN own_or_rent_flag SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN employer SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.silver.silver_customer MODIFY
  COLUMN last_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN first_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middle_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN gender SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN postal_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN state_province SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN country SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN primary_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN alternate_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN tax_id SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_country_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_area_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_extension SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_country_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_area_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_extension SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_country_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_area_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_extension SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.silver.silver_customer MODIFY
  COLUMN date_of_birth SET MASKING POLICY brokerage_dwh.governance.mask_pii_date;

ALTER TABLE brokerage_dwh.silver.silver_trade MODIFY
  COLUMN execution_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.silver.silver_prospect MODIFY
  COLUMN income SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN number_cars SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN number_children SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN age SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN credit_rating SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN number_credit_cards SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN net_worth SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric;
  
-- ---------------------------------------------------------------------------
-- Apply to gold dim_customer's restricted_pii columns (once built —
-- placeholder here, adjust to actual gold column names)
-- ---------------------------------------------------------------------------
USE SCHEMA brokerage_dwh.gold;

ALTER TABLE brokerage_dwh.gold.dim_broker MODIFY
  COLUMN first_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN last_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middle_initial SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.gold.dim_customer MODIFY
  COLUMN tax_id SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN last_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN first_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middle_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN gender SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN postal_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN state_province SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN country SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_country_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_area_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_local_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone1_extension SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_country_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_area_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_local_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone2_extension SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_country_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_area_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_local_number SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone3_extension SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN primary_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN alternate_email SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.gold.dim_customer MODIFY
  COLUMN date_of_birth SET MASKING POLICY brokerage_dwh.governance.mask_pii_date;

ALTER TABLE brokerage_dwh.gold.dim_prospect MODIFY
  COLUMN last_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN first_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN middle_initial SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN gender SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line1 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN address_line2 SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN postal_code SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN city SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN state SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN country SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN phone SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN marital_status SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN own_or_rent_flag SET MASKING POLICY brokerage_dwh.governance.mask_pii_string,
  COLUMN employer_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.gold.fact_trade MODIFY
  COLUMN executor_name SET MASKING POLICY brokerage_dwh.governance.mask_pii_string;

ALTER TABLE brokerage_dwh.gold.dim_prospect MODIFY
  COLUMN annual_income SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN number_of_cars SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN number_of_children SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN age SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN credit_rating SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN number_of_credit_cards SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric,
  COLUMN net_worth SET MASKING POLICY brokerage_dwh.governance.mask_pii_numeric;