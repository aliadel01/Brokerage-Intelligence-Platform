-- ============================================================================
-- Data Classification — Snowflake tag setup + column-level tagging
-- Run once per environment. ALTER TABLE ... MODIFY COLUMN ... SET TAG
-- is idempotent (safe to re-run).
-- ============================================================================

USE SCHEMA brokerage_dwh.bronze;

CREATE TAG IF NOT EXISTS data_classification
    ALLOWED_VALUES 'public', 'internal', 'confidential', 'restricted_pii';

-- ---------------------------------------------------------------------------
-- Archetype A — static reference dimensions (mostly public/internal)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_date MODIFY COLUMN sk_dateid SET TAG data_classification = 'public';
-- Rest of bronze_date: calendar/fiscal descriptors, no PII/financial risk
ALTER TABLE bronze_date MODIFY COLUMN datevalue SET TAG data_classification = 'public';

ALTER TABLE bronze_time MODIFY COLUMN sk_timeid SET TAG data_classification = 'public';

ALTER TABLE bronze_status_type MODIFY COLUMN st_id SET TAG data_classification = 'public';

ALTER TABLE bronze_tax_rate MODIFY COLUMN tx_id SET TAG data_classification = 'internal';
ALTER TABLE bronze_tax_rate MODIFY COLUMN tx_rate SET TAG data_classification = 'internal';

ALTER TABLE bronze_industry MODIFY COLUMN in_id SET TAG data_classification = 'public';

ALTER TABLE bronze_trade_type MODIFY COLUMN tt_id SET TAG data_classification = 'public';

-- HR: employee PII (names, phone) — restricted_pii
ALTER TABLE bronze_hr MODIFY COLUMN employeefirstname SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeelastname  SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeemi        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeephone     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeeid        SET TAG data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN managerid         SET TAG data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN employeejobcode   SET TAG data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN employeebranch    SET TAG data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN employeeoffice    SET TAG data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Archetype B — real CDC (account/customer/trade)
-- ---------------------------------------------------------------------------

-- bronze_account: business identifiers, not PII — confidential (SOX-adjacent)
ALTER TABLE bronze_account MODIFY COLUMN ca_id     SET TAG data_classification = 'confidential';
ALTER TABLE bronze_account MODIFY COLUMN ca_c_id   SET TAG data_classification = 'confidential';
ALTER TABLE bronze_account MODIFY COLUMN ca_b_id   SET TAG data_classification = 'internal';
ALTER TABLE bronze_account MODIFY COLUMN ca_name   SET TAG data_classification = 'confidential';
ALTER TABLE bronze_account MODIFY COLUMN ca_tax_st SET TAG data_classification = 'internal';
ALTER TABLE bronze_account MODIFY COLUMN ca_st_id  SET TAG data_classification = 'internal';

-- bronze_customer: heavy PII surface
ALTER TABLE bronze_customer MODIFY COLUMN c_id          SET TAG data_classification = 'confidential';
ALTER TABLE bronze_customer MODIFY COLUMN c_tax_id      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_l_name      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_f_name      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_m_name      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_gndr        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_dob         SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_adline1     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_adline2     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_zipcode     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_city        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_state_prov  SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_prim_email  SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_alt_email   SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry_1      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_area_1      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_local_1     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ext_1       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry_2      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_area_2      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_local_2     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ext_2       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry_3      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_area_3      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_local_3     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ext_3       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_tier        SET TAG data_classification = 'confidential';
ALTER TABLE bronze_customer MODIFY COLUMN c_st_id       SET TAG data_classification = 'internal';
ALTER TABLE bronze_customer MODIFY COLUMN c_lcl_tx_id   SET TAG data_classification = 'internal';
ALTER TABLE bronze_customer MODIFY COLUMN c_nat_tx_id   SET TAG data_classification = 'internal';

-- bronze_trade: financial, SOX-relevant — confidential, not PII
ALTER TABLE bronze_trade MODIFY COLUMN t_id          SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_ca_id       SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_bid_price   SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_trade_price SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_chrg        SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_comm        SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_tax         SET TAG data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_exec_name   SET TAG data_classification = 'restricted_pii';

-- ---------------------------------------------------------------------------
-- Archetype B — quasi-CDC (financial event logs, not PII)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_holding_history  MODIFY COLUMN hh_t_id      SET TAG data_classification = 'confidential';
ALTER TABLE bronze_watch_history    MODIFY COLUMN w_c_id       SET TAG data_classification = 'confidential';
ALTER TABLE bronze_daily_market     MODIFY COLUMN dm_close     SET TAG data_classification = 'internal';
ALTER TABLE bronze_cash_transaction MODIFY COLUMN ct_ca_id     SET TAG data_classification = 'confidential';
ALTER TABLE bronze_cash_transaction MODIFY COLUMN ct_amt       SET TAG data_classification = 'confidential';
ALTER TABLE bronze_cash_transaction MODIFY COLUMN ct_name      SET TAG data_classification = 'confidential';

-- ---------------------------------------------------------------------------
-- Archetype C — prospect (marketing PII, income/finance)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_prospect MODIFY COLUMN lastname       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN firstname      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN middleinitial  SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN addressline1   SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN addressline2   SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN postalcode     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN city           SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN state          SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN country        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN phone          SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN gender         SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN income         SET TAG data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN creditrating   SET TAG data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN networth       SET TAG data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN employer       SET TAG data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN agencyid       SET TAG data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Archetype D — FINWIRE (public-company financial disclosures — confidential
-- until published, not personal PII; ceoname is a named individual but tied
-- to a public corporate role, kept confidential not restricted_pii)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_finwire_cmp MODIFY COLUMN companyname SET TAG data_classification = 'confidential';
ALTER TABLE bronze_finwire_cmp MODIFY COLUMN ceoname     SET TAG data_classification = 'confidential';
ALTER TABLE bronze_finwire_cmp MODIFY COLUMN cik         SET TAG data_classification = 'confidential';

ALTER TABLE bronze_finwire_sec MODIFY COLUMN symbol      SET TAG data_classification = 'internal';
ALTER TABLE bronze_finwire_sec MODIFY COLUMN dividend    SET TAG data_classification = 'confidential';

ALTER TABLE bronze_finwire_fin MODIFY COLUMN revenue     SET TAG data_classification = 'confidential';
ALTER TABLE bronze_finwire_fin MODIFY COLUMN earnings    SET TAG data_classification = 'confidential';
ALTER TABLE bronze_finwire_fin MODIFY COLUMN eps         SET TAG data_classification = 'confidential';

-- ---------------------------------------------------------------------------
-- Archetype D — CustomerMgmt.xml (same PII surface as bronze_customer)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_id          SET TAG data_classification = 'confidential';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_tax_id      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_l_name      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_f_name      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_m_name      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_gndr        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_dob         SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_adline1     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_adline2     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_zipcode     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_city        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_state_prov  SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry        SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_prim_email  SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_alt_email   SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry_1      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_area_1      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_local_1     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ext_1       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry_2      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_area_2      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_local_2     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ext_2       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry_3      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_area_3      SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_local_3     SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ext_3       SET TAG data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_tier        SET TAG data_classification = 'confidential';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_lcl_tx_id   SET TAG data_classification = 'internal';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_nat_tx_id   SET TAG data_classification = 'internal';

ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_id     SET TAG data_classification = 'confidential';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN c_id      SET TAG data_classification = 'confidential';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_name   SET TAG data_classification = 'confidential';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_tax_st SET TAG data_classification = 'internal';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_b_id   SET TAG data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Archetype E — trade_history (financial, confidential)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_trade_history MODIFY COLUMN th_t_id SET TAG data_classification = 'confidential';

-- ---------------------------------------------------------------------------
-- Operational / control tables — internal, not PII
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_batch_control MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE bronze_batch_control MODIFY COLUMN asofdate  SET TAG data_classification = 'internal';

ALTER TABLE bronze_source_audit MODIFY COLUMN dataset   SET TAG data_classification = 'internal';
ALTER TABLE bronze_source_audit MODIFY COLUMN attribute SET TAG data_classification = 'internal';
ALTER TABLE bronze_source_audit MODIFY COLUMN value     SET TAG data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Silver tables — same classification as bronze archetypes
-- ---------------------------------------------------------------------------
USE SCHEMA brokerage_dwh.silver;

-- silver_date
ALTER TABLE silver_date MODIFY COLUMN date_sk SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN date_value SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN date_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_year_id SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_year_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_qtr_id SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_qtr_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_month_id SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_month_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_week_id SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN calendar_week_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN day_of_week_num SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN day_of_week_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN fiscal_year_id SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN fiscal_year_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN fiscal_qtr_id SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN fiscal_qtr_desc SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN holiday_flag SET TAG data_classification = 'public';
ALTER TABLE silver_date MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_date MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_hr
ALTER TABLE silver_hr MODIFY COLUMN employee_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_hr MODIFY COLUMN manager_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_hr MODIFY COLUMN first_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_hr MODIFY COLUMN last_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_hr MODIFY COLUMN middle_initial SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_hr MODIFY COLUMN job_code SET TAG data_classification = 'internal';
ALTER TABLE silver_hr MODIFY COLUMN branch SET TAG data_classification = 'internal';
ALTER TABLE silver_hr MODIFY COLUMN office SET TAG data_classification = 'internal';
ALTER TABLE silver_hr MODIFY COLUMN phone SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_hr MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_hr MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_industry
ALTER TABLE silver_industry MODIFY COLUMN industry_id SET TAG data_classification = 'public';
ALTER TABLE silver_industry MODIFY COLUMN industry_name SET TAG data_classification = 'public';
ALTER TABLE silver_industry MODIFY COLUMN sector_id SET TAG data_classification = 'public';
ALTER TABLE silver_industry MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_industry MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_status_type
ALTER TABLE silver_status_type MODIFY COLUMN status_id SET TAG data_classification = 'public';
ALTER TABLE silver_status_type MODIFY COLUMN status_name SET TAG data_classification = 'public';
ALTER TABLE silver_status_type MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_status_type MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_tax_rate
ALTER TABLE silver_tax_rate MODIFY COLUMN tax_rate_id SET TAG data_classification = 'public';
ALTER TABLE silver_tax_rate MODIFY COLUMN tax_rate_name SET TAG data_classification = 'public';
ALTER TABLE silver_tax_rate MODIFY COLUMN tax_rate SET TAG data_classification = 'public';
ALTER TABLE silver_tax_rate MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_tax_rate MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_time
ALTER TABLE silver_time MODIFY COLUMN time_sk SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN time_value SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN hour_id SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN hour_desc SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN minute_id SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN minute_desc SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN second_id SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN second_desc SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN market_hours_flag SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN office_hours_flag SET TAG data_classification = 'public';
ALTER TABLE silver_time MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_time MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_trade_type
ALTER TABLE silver_trade_type MODIFY COLUMN trade_type_id SET TAG data_classification = 'public';
ALTER TABLE silver_trade_type MODIFY COLUMN trade_type_name SET TAG data_classification = 'public';
ALTER TABLE silver_trade_type MODIFY COLUMN is_sell_flag SET TAG data_classification = 'public';
ALTER TABLE silver_trade_type MODIFY COLUMN is_market_flag SET TAG data_classification = 'public';
ALTER TABLE silver_trade_type MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_type MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_prospect
ALTER TABLE silver_prospect MODIFY COLUMN agency_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_prospect MODIFY COLUMN last_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN first_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN middle_initial SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN gender SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN address_line1 SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN address_line2 SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN postal_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN city SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN state SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN country SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN phone SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN income SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN number_cars SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN number_children SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN marital_status SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN age SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN credit_rating SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN own_or_rent_flag SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN employer SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN number_credit_cards SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN net_worth SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_prospect MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_prospect MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_prospect MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_account
ALTER TABLE silver_account MODIFY COLUMN account_version_sk SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN account_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_account MODIFY COLUMN broker_id SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN customer_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_account MODIFY COLUMN account_name SET TAG data_classification = 'confidential';
ALTER TABLE silver_account MODIFY COLUMN tax_status SET TAG data_classification = 'confidential';
ALTER TABLE silver_account MODIFY COLUMN status_id SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN valid_from_date SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN valid_to_date SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN is_current SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_account MODIFY COLUMN _source_table SET TAG data_classification = 'internal';

-- silver_customer
ALTER TABLE silver_customer MODIFY COLUMN customer_version_sk SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN customer_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_customer MODIFY COLUMN last_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN first_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN middle_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN gender SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN tier SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN date_of_birth SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN address_line1 SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN address_line2 SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN postal_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN city SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN state_province SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN country SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN primary_email SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN alternate_email SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN tax_id SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN local_tax_rate_id SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN national_tax_rate_id SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN phone1_country_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone1_area_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone1_number SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone1_extension SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone2_country_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone2_area_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone2_number SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone2_extension SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone3_country_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone3_area_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone3_number SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN phone3_extension SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_customer MODIFY COLUMN status_id SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN valid_from_date SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN valid_to_date SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN is_current SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_customer MODIFY COLUMN _source_table SET TAG data_classification = 'internal';

-- silver_trade
ALTER TABLE silver_trade MODIFY COLUMN trade_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN status_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN trade_type_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN is_cash SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN symbol SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN quantity SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN bid_price SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN customer_account_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_trade MODIFY COLUMN execution_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE silver_trade MODIFY COLUMN trade_price SET TAG data_classification = 'confidential';
ALTER TABLE silver_trade MODIFY COLUMN charge SET TAG data_classification = 'confidential';
ALTER TABLE silver_trade MODIFY COLUMN commission SET TAG data_classification = 'confidential';
ALTER TABLE silver_trade MODIFY COLUMN tax SET TAG data_classification = 'confidential';
ALTER TABLE silver_trade MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN trade_timestamp SET TAG data_classification = 'internal';
ALTER TABLE silver_trade MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';

-- silver_finwire_cmp
ALTER TABLE silver_finwire_cmp MODIFY COLUMN posting_ts SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN company_name SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN cik SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN status SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN industry_id SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN sp_rating SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN founding_date SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN address_line1 SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN address_line2 SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN postal_code SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN city SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN state_province SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN country SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN ceo_name SET TAG data_classification = 'confidential';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN description SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_cmp MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_finwire_sec
ALTER TABLE silver_finwire_sec MODIFY COLUMN posting_ts SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_sec MODIFY COLUMN security_symbol SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN issue_type SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN status SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN security_name SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN exchange_id SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN shares_outstanding SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN first_trade_date SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN first_trade_exchange_date SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN dividend SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN company_name SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN company_cik SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_sec MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_sec MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_sec MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_cash_transaction
ALTER TABLE silver_cash_transaction MODIFY COLUMN account_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_cash_transaction MODIFY COLUMN transaction_ts SET TAG data_classification = 'internal';
ALTER TABLE silver_cash_transaction MODIFY COLUMN amount SET TAG data_classification = 'confidential';
ALTER TABLE silver_cash_transaction MODIFY COLUMN description SET TAG data_classification = 'confidential';
ALTER TABLE silver_cash_transaction MODIFY COLUMN _cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_cash_transaction MODIFY COLUMN _cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE silver_cash_transaction MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_cash_transaction MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_cash_transaction MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_daily_market
ALTER TABLE silver_daily_market MODIFY COLUMN market_date SET TAG data_classification = 'public';
ALTER TABLE silver_daily_market MODIFY COLUMN security_symbol SET TAG data_classification = 'public';
ALTER TABLE silver_daily_market MODIFY COLUMN close_price SET TAG data_classification = 'public';
ALTER TABLE silver_daily_market MODIFY COLUMN high_price SET TAG data_classification = 'public';
ALTER TABLE silver_daily_market MODIFY COLUMN low_price SET TAG data_classification = 'public';
ALTER TABLE silver_daily_market MODIFY COLUMN volume SET TAG data_classification = 'public';
ALTER TABLE silver_daily_market MODIFY COLUMN _cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_daily_market MODIFY COLUMN _cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE silver_daily_market MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_daily_market MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_daily_market MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_holding_history
ALTER TABLE silver_holding_history MODIFY COLUMN originating_trade_id SET TAG data_classification = 'internal';
ALTER TABLE silver_holding_history MODIFY COLUMN trade_id SET TAG data_classification = 'internal';
ALTER TABLE silver_holding_history MODIFY COLUMN qty_before SET TAG data_classification = 'confidential';
ALTER TABLE silver_holding_history MODIFY COLUMN qty_after SET TAG data_classification = 'confidential';
ALTER TABLE silver_holding_history MODIFY COLUMN _cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_holding_history MODIFY COLUMN _cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE silver_holding_history MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_holding_history MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_holding_history MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_watch_history
ALTER TABLE silver_watch_history MODIFY COLUMN customer_id SET TAG data_classification = 'confidential';
ALTER TABLE silver_watch_history MODIFY COLUMN security_symbol SET TAG data_classification = 'public';
ALTER TABLE silver_watch_history MODIFY COLUMN event_ts SET TAG data_classification = 'internal';
ALTER TABLE silver_watch_history MODIFY COLUMN watch_action SET TAG data_classification = 'internal';
ALTER TABLE silver_watch_history MODIFY COLUMN _cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE silver_watch_history MODIFY COLUMN _cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE silver_watch_history MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_watch_history MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_watch_history MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_finwire_fin
ALTER TABLE silver_finwire_fin MODIFY COLUMN posting_ts SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_fin MODIFY COLUMN year SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN quarter SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN qtr_start_date SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN posting_date SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN revenue SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN earnings SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN eps SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN diluted_eps SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN margin SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN inventory SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN assets SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN liabilities SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN shares_outstanding SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN diluted_shares_outstanding SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN company_name SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN company_cik SET TAG data_classification = 'public';
ALTER TABLE silver_finwire_fin MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_fin MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_finwire_fin MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';

-- silver_trade_history
ALTER TABLE silver_trade_history MODIFY COLUMN trade_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_history MODIFY COLUMN status_ts SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_history MODIFY COLUMN status_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_history MODIFY COLUMN _batch_id SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_history MODIFY COLUMN _row_hash SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_history MODIFY COLUMN _loaded_at SET TAG data_classification = 'internal';
ALTER TABLE silver_trade_history MODIFY COLUMN _source_model SET TAG data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Gold tables — same classification as silver archetypes
-- ---------------------------------------------------------------------------
USE SCHEMA brokerage_dwh.gold;

-- dim_account
ALTER TABLE dim_account MODIFY COLUMN account_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN account_id SET TAG data_classification = 'confidential';
ALTER TABLE dim_account MODIFY COLUMN account_name SET TAG data_classification = 'confidential';
ALTER TABLE dim_account MODIFY COLUMN tax_status SET TAG data_classification = 'confidential';
ALTER TABLE dim_account MODIFY COLUMN account_status SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN broker_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN customer_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN effective_start_date SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN effective_end_date SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN is_current SET TAG data_classification = 'internal';
ALTER TABLE dim_account MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';

-- dim_broker
ALTER TABLE dim_broker MODIFY COLUMN broker_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_broker MODIFY COLUMN employee_id SET TAG data_classification = 'confidential';
ALTER TABLE dim_broker MODIFY COLUMN manager_employee_id SET TAG data_classification = 'confidential';
ALTER TABLE dim_broker MODIFY COLUMN first_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_broker MODIFY COLUMN last_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_broker MODIFY COLUMN middle_initial SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_broker MODIFY COLUMN job_code SET TAG data_classification = 'internal';
ALTER TABLE dim_broker MODIFY COLUMN branch_name SET TAG data_classification = 'internal';
ALTER TABLE dim_broker MODIFY COLUMN office_code SET TAG data_classification = 'internal';
ALTER TABLE dim_broker MODIFY COLUMN phone_number SET TAG data_classification = 'restricted_pii';

-- dim_company
ALTER TABLE dim_company MODIFY COLUMN company_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_company MODIFY COLUMN company_cik SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN company_name SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN company_status SET TAG data_classification = 'internal';
ALTER TABLE dim_company MODIFY COLUMN sp_rating SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN founding_date SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN address_line1 SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN address_line2 SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN postal_code SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN city SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN state_province SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN country SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN ceo_name SET TAG data_classification = 'confidential';
ALTER TABLE dim_company MODIFY COLUMN company_description SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN industry_code SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN industry_name SET TAG data_classification = 'public';
ALTER TABLE dim_company MODIFY COLUMN sector_id SET TAG data_classification = 'public';

-- dim_customer
ALTER TABLE dim_customer MODIFY COLUMN customer_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN customer_id SET TAG data_classification = 'confidential';
ALTER TABLE dim_customer MODIFY COLUMN tax_id SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN customer_status SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN last_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN first_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN middle_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN gender SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN customer_tier SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN date_of_birth SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN address_line1 SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN address_line2 SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN postal_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN city SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN state_province SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN country SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone1_country_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone1_area_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone1_local_number SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone1_extension SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone2_country_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone2_area_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone2_local_number SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone2_extension SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone3_country_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone3_area_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone3_local_number SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN phone3_extension SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN primary_email SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN alternate_email SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_customer MODIFY COLUMN effective_start_date SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN effective_end_date SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN is_current SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN local_tax_rate_code SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN local_tax_rate_name SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN local_tax_rate_pct SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN national_tax_rate_code SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN national_tax_rate_name SET TAG data_classification = 'internal';
ALTER TABLE dim_customer MODIFY COLUMN national_tax_rate_pct SET TAG data_classification = 'internal';

-- dim_date
ALTER TABLE dim_date MODIFY COLUMN date_sk SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN date_value SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN date_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_year SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_year_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_qtr_id SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_qtr_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_month_id SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_month_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_week_id SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN calendar_week_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN day_of_week_num SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN day_of_week_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN fiscal_year SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN fiscal_year_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN fiscal_qtr_id SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN fiscal_qtr_desc SET TAG data_classification = 'public';
ALTER TABLE dim_date MODIFY COLUMN is_holiday SET TAG data_classification = 'public';

-- dim_prospect
ALTER TABLE dim_prospect MODIFY COLUMN prospect_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_prospect MODIFY COLUMN customer_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_prospect MODIFY COLUMN agency_id SET TAG data_classification = 'confidential';
ALTER TABLE dim_prospect MODIFY COLUMN last_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN first_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN middle_initial SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN gender SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN address_line1 SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN address_line2 SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN postal_code SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN city SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN state SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN country SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN phone SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN annual_income SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN number_of_cars SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN number_of_children SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN marital_status SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN age SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN credit_rating SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN own_or_rent_flag SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN employer_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN number_of_credit_cards SET TAG data_classification = 'restricted_pii';
ALTER TABLE dim_prospect MODIFY COLUMN net_worth SET TAG data_classification = 'restricted_pii';

-- dim_security
ALTER TABLE dim_security MODIFY COLUMN security_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_security MODIFY COLUMN symbol SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN issue_type SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN security_status SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN security_name SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN exchange_id SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN shares_outstanding SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN first_trade_date SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN first_trade_exchange_date SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN dividend SET TAG data_classification = 'public';
ALTER TABLE dim_security MODIFY COLUMN company_sk SET TAG data_classification = 'internal';

-- dim_statustype
ALTER TABLE dim_statustype MODIFY COLUMN status_type_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_statustype MODIFY COLUMN status_code SET TAG data_classification = 'public';
ALTER TABLE dim_statustype MODIFY COLUMN status_name SET TAG data_classification = 'public';

-- dim_time
ALTER TABLE dim_time MODIFY COLUMN time_sk SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN time_value SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN hour_id SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN hour_desc SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN minute_id SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN minute_desc SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN second_id SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN second_desc SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN is_market_hours SET TAG data_classification = 'public';
ALTER TABLE dim_time MODIFY COLUMN is_office_hours SET TAG data_classification = 'public';

-- dim_tradetype
ALTER TABLE dim_tradetype MODIFY COLUMN trade_type_sk SET TAG data_classification = 'internal';
ALTER TABLE dim_tradetype MODIFY COLUMN trade_type_code SET TAG data_classification = 'public';
ALTER TABLE dim_tradetype MODIFY COLUMN trade_type_name SET TAG data_classification = 'public';
ALTER TABLE dim_tradetype MODIFY COLUMN is_sell_flag SET TAG data_classification = 'public';
ALTER TABLE dim_tradetype MODIFY COLUMN is_market_order_flag SET TAG data_classification = 'public';

-- fact_cash_transaction
ALTER TABLE fact_cash_transaction MODIFY COLUMN transaction_date_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_cash_transaction MODIFY COLUMN transaction_time_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_cash_transaction MODIFY COLUMN account_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_cash_transaction MODIFY COLUMN amount SET TAG data_classification = 'confidential';
ALTER TABLE fact_cash_transaction MODIFY COLUMN description SET TAG data_classification = 'confidential';
ALTER TABLE fact_cash_transaction MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE fact_cash_transaction MODIFY COLUMN cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE fact_cash_transaction MODIFY COLUMN batch_id SET TAG data_classification = 'internal';

-- fact_company_financials
ALTER TABLE fact_company_financials MODIFY COLUMN company_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_company_financials MODIFY COLUMN fiscal_date_sk SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN fiscal_year SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN fiscal_quarter SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN revenue SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN earnings SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN eps_basic SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN eps_diluted SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN profit_margin SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN inventory SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN total_assets SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN total_liabilities SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN shares_outstanding SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN diluted_shares_outstanding SET TAG data_classification = 'public';
ALTER TABLE fact_company_financials MODIFY COLUMN batch_id SET TAG data_classification = 'internal';
ALTER TABLE fact_company_financials MODIFY COLUMN posting_date_sk SET TAG data_classification = 'internal';

-- fact_holding
ALTER TABLE fact_holding MODIFY COLUMN originating_trade_id SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN current_trade_id SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN security_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN holding_date_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN account_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN before_quantity SET TAG data_classification = 'confidential';
ALTER TABLE fact_holding MODIFY COLUMN after_quantity SET TAG data_classification = 'confidential';
ALTER TABLE fact_holding MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE fact_holding MODIFY COLUMN batch_id SET TAG data_classification = 'internal';

-- fact_market_history
ALTER TABLE fact_market_history MODIFY COLUMN security_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_market_history MODIFY COLUMN market_date_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_market_history MODIFY COLUMN close_price SET TAG data_classification = 'public';
ALTER TABLE fact_market_history MODIFY COLUMN high_price SET TAG data_classification = 'public';
ALTER TABLE fact_market_history MODIFY COLUMN low_price SET TAG data_classification = 'public';
ALTER TABLE fact_market_history MODIFY COLUMN volume SET TAG data_classification = 'public';
ALTER TABLE fact_market_history MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE fact_market_history MODIFY COLUMN cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE fact_market_history MODIFY COLUMN batch_id SET TAG data_classification = 'internal';

-- fact_trade
ALTER TABLE fact_trade MODIFY COLUMN trade_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN trade_id SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN security_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN trade_date_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN trade_time_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN status_type_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN trade_type_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN is_cash_flag SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN quantity SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN bid_price SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN account_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN executor_name SET TAG data_classification = 'restricted_pii';
ALTER TABLE fact_trade MODIFY COLUMN trade_price SET TAG data_classification = 'confidential';
ALTER TABLE fact_trade MODIFY COLUMN charge_amount SET TAG data_classification = 'confidential';
ALTER TABLE fact_trade MODIFY COLUMN commission_amount SET TAG data_classification = 'confidential';
ALTER TABLE fact_trade MODIFY COLUMN tax_amount SET TAG data_classification = 'confidential';
ALTER TABLE fact_trade MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE fact_trade MODIFY COLUMN batch_id SET TAG data_classification = 'internal';

-- fact_trade_history
ALTER TABLE fact_trade_history MODIFY COLUMN trade_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN trade_id SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN account_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN security_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN status_type_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN status_date_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN status_time_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN source_model SET TAG data_classification = 'internal';
ALTER TABLE fact_trade_history MODIFY COLUMN batch_id SET TAG data_classification = 'internal';

-- fact_watch_history
ALTER TABLE fact_watch_history MODIFY COLUMN customer_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN security_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN watch_date_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN watch_time_sk SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN action_code SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN cdc_flag SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN cdc_dsn SET TAG data_classification = 'internal';
ALTER TABLE fact_watch_history MODIFY COLUMN batch_id SET TAG data_classification = 'internal';