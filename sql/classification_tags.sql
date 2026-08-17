-- ============================================================================
-- Data Classification — Snowflake tag setup + column-level tagging
-- Run once per environment. ALTER TABLE ... MODIFY COLUMN ... SET TAG
-- is idempotent (safe to re-run).
-- ============================================================================


CREATE TAG IF NOT EXISTS brokerage_dwh.governance.data_classification
    ALLOWED_VALUES 'public', 'internal', 'confidential', 'restricted_pii';

-- ---------------------------------------------------------------------------
-- Archetype A — static reference dimensions (mostly public/internal)
-- ---------------------------------------------------------------------------
USE SCHEMA brokerage_dwh.bronze;

ALTER TABLE bronze_date MODIFY COLUMN sk_dateid SET TAG brokerage_dwh.governance.data_classification = 'public';
-- Rest of bronze_date: calendar/fiscal descriptors, no PII/financial risk
ALTER TABLE bronze_date MODIFY COLUMN datevalue SET TAG brokerage_dwh.governance.data_classification = 'public';

ALTER TABLE bronze_time MODIFY COLUMN sk_timeid SET TAG brokerage_dwh.governance.data_classification = 'public';

ALTER TABLE bronze_status_type MODIFY COLUMN st_id SET TAG brokerage_dwh.governance.data_classification = 'public';

ALTER TABLE bronze_tax_rate MODIFY COLUMN tx_id SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_tax_rate MODIFY COLUMN tx_rate SET TAG brokerage_dwh.governance.data_classification = 'internal';

ALTER TABLE bronze_industry MODIFY COLUMN in_id SET TAG brokerage_dwh.governance.data_classification = 'public';

ALTER TABLE bronze_trade_type MODIFY COLUMN tt_id SET TAG brokerage_dwh.governance.data_classification = 'public';

-- HR: employee PII (names, phone) — restricted_pii
ALTER TABLE bronze_hr MODIFY COLUMN employeefirstname SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeelastname  SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeemi        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeephone     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_hr MODIFY COLUMN employeeid        SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN managerid         SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN employeejobcode   SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN employeebranch    SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_hr MODIFY COLUMN employeeoffice    SET TAG brokerage_dwh.governance.data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Archetype B — real CDC (account/customer/trade)
-- ---------------------------------------------------------------------------

-- bronze_account: business identifiers, not PII — confidential (SOX-adjacent)
ALTER TABLE bronze_account MODIFY COLUMN ca_id     SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_account MODIFY COLUMN ca_c_id   SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_account MODIFY COLUMN ca_b_id   SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_account MODIFY COLUMN ca_name   SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_account MODIFY COLUMN ca_tax_st SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_account MODIFY COLUMN ca_st_id  SET TAG brokerage_dwh.governance.data_classification = 'internal';

-- bronze_customer: heavy PII surface
ALTER TABLE bronze_customer MODIFY COLUMN c_id          SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_customer MODIFY COLUMN c_tax_id      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_l_name      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_f_name      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_m_name      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_gndr        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_dob         SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_adline1     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_adline2     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_zipcode     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_city        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_state_prov  SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_prim_email  SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_alt_email   SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry_1      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_area_1      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_local_1     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ext_1       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry_2      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_area_2      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_local_2     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ext_2       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ctry_3      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_area_3      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_local_3     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_ext_3       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_customer MODIFY COLUMN c_tier        SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_customer MODIFY COLUMN c_st_id       SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_customer MODIFY COLUMN c_lcl_tx_id   SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_customer MODIFY COLUMN c_nat_tx_id   SET TAG brokerage_dwh.governance.data_classification = 'internal';

-- bronze_trade: financial, SOX-relevant — confidential, not PII
ALTER TABLE bronze_trade MODIFY COLUMN t_id          SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_ca_id       SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_bid_price   SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_trade_price SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_chrg        SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_comm        SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_tax         SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_trade MODIFY COLUMN t_exec_name   SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';

-- ---------------------------------------------------------------------------
-- Archetype B — quasi-CDC (financial event logs, not PII)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_holding_history  MODIFY COLUMN hh_t_id      SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_watch_history    MODIFY COLUMN w_c_id       SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_daily_market     MODIFY COLUMN dm_close     SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_cash_transaction MODIFY COLUMN ct_ca_id     SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_cash_transaction MODIFY COLUMN ct_amt       SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_cash_transaction MODIFY COLUMN ct_name      SET TAG brokerage_dwh.governance.data_classification = 'confidential';

-- ---------------------------------------------------------------------------
-- Archetype C — prospect (marketing PII, income/finance)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_prospect MODIFY COLUMN lastname       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN firstname      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN middleinitial  SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN addressline1   SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN addressline2   SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN postalcode     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN city           SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN state          SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN country        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN phone          SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN gender         SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_prospect MODIFY COLUMN income         SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN creditrating   SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN networth       SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN employer       SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_prospect MODIFY COLUMN agencyid       SET TAG brokerage_dwh.governance.data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Archetype D — FINWIRE (public-company financial disclosures — confidential
-- until published, not personal PII; ceoname is a named individual but tied
-- to a public corporate role, kept confidential not restricted_pii)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_finwire_cmp MODIFY COLUMN companyname SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_finwire_cmp MODIFY COLUMN ceoname     SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_finwire_cmp MODIFY COLUMN cik         SET TAG brokerage_dwh.governance.data_classification = 'confidential';

ALTER TABLE bronze_finwire_sec MODIFY COLUMN symbol      SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_finwire_sec MODIFY COLUMN dividend    SET TAG brokerage_dwh.governance.data_classification = 'confidential';

ALTER TABLE bronze_finwire_fin MODIFY COLUMN revenue     SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_finwire_fin MODIFY COLUMN earnings    SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_finwire_fin MODIFY COLUMN eps         SET TAG brokerage_dwh.governance.data_classification = 'confidential';

-- ---------------------------------------------------------------------------
-- Archetype D — CustomerMgmt.xml (same PII surface as bronze_customer)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_id          SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_tax_id      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_l_name      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_f_name      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_m_name      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_gndr        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_dob         SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_adline1     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_adline2     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_zipcode     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_city        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_state_prov  SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry        SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_prim_email  SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_alt_email   SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry_1      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_area_1      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_local_1     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ext_1       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry_2      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_area_2      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_local_2     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ext_2       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ctry_3      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_area_3      SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_local_3     SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_ext_3       SET TAG brokerage_dwh.governance.data_classification = 'restricted_pii';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_tier        SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_lcl_tx_id   SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_mgmt_customer MODIFY COLUMN c_nat_tx_id   SET TAG brokerage_dwh.governance.data_classification = 'internal';

ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_id     SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN c_id      SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_name   SET TAG brokerage_dwh.governance.data_classification = 'confidential';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_tax_st SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_mgmt_account MODIFY COLUMN ca_b_id   SET TAG brokerage_dwh.governance.data_classification = 'internal';

-- ---------------------------------------------------------------------------
-- Archetype E — trade_history (financial, confidential)
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_trade_history MODIFY COLUMN th_t_id SET TAG brokerage_dwh.governance.data_classification = 'confidential';

-- ---------------------------------------------------------------------------
-- Operational / control tables — internal, not PII
-- ---------------------------------------------------------------------------

ALTER TABLE bronze_batch_control MODIFY COLUMN _batch_id SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_batch_control MODIFY COLUMN asofdate  SET TAG brokerage_dwh.governance.data_classification = 'internal';

ALTER TABLE bronze_source_audit MODIFY COLUMN dataset   SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_source_audit MODIFY COLUMN attribute SET TAG brokerage_dwh.governance.data_classification = 'internal';
ALTER TABLE bronze_source_audit MODIFY COLUMN value     SET TAG brokerage_dwh.governance.data_classification = 'internal';