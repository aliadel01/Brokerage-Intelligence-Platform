
-- ---------------------------------------------------------------------------
-- Masking policies — applied to restricted_pii columns in silver/gold.
-- role_dbt_prod_ci, role_custodian and ACCOUNTADMIN see real values; every other role
-- (including role_analyst) sees the masked form.
-- ---------------------------------------------------------------------------

CREATE MASKING POLICY IF NOT EXISTS brokerage_dwh.governance.mask_pii_string AS (val VARCHAR)
    RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_CUSTODIAN', 'ACCOUNTADMIN', 'ROLE_DBT_PROD_CI') THEN val
        ELSE '***MASKED***'
    END;

CREATE MASKING POLICY IF NOT EXISTS brokerage_dwh.governance.mask_pii_date AS (val DATE)
    RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_CUSTODIAN', 'ACCOUNTADMIN', 'ROLE_DBT_PROD_CI') THEN val
        ELSE NULL
    END;

CREATE MASKING POLICY IF NOT EXISTS brokerage_dwh.governance.mask_pii_numeric AS (val NUMBER)
    RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_CUSTODIAN', 'ACCOUNTADMIN', 'ROLE_DBT_PROD_CI') THEN val
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

-- bronze_mgmt_customer 
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