-- ============================================================================
-- Role & access design — brokerage-data-platform
-- Least-privilege, per Segregation of Duties principles.
-- Run as ACCOUNTADMIN or a role with role/user management privileges.
-- Governance objects (dq_audit_log, masking policies) live in governance.sql
-- — this file only grants roles access to them, it doesn't create them.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. role_bronze_loader — bronze ingestion, service account only
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS role_bronze_loader;

CREATE USER IF NOT EXISTS svc_bronze_loader
    PASSWORD = '<strong-generated-secret>'  
    DEFAULT_ROLE = role_bronze_loader
    DEFAULT_WAREHOUSE = compute_wh
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for bronze ingestion pipeline only. No human login.';

GRANT ROLE role_bronze_loader TO USER svc_bronze_loader;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE role_bronze_loader;
GRANT USAGE ON DATABASE brokerage_dwh TO ROLE role_bronze_loader;
GRANT USAGE ON SCHEMA brokerage_dwh.bronze TO ROLE role_bronze_loader;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA brokerage_dwh.bronze TO ROLE role_bronze_loader;
GRANT SELECT, INSERT, DELETE ON FUTURE TABLES IN SCHEMA brokerage_dwh.bronze TO ROLE role_bronze_loader;
GRANT READ, WRITE ON STAGE brokerage_dwh.bronze.ingest_stage TO ROLE role_bronze_loader;
GRANT USAGE ON FILE FORMAT brokerage_dwh.bronze.ff_bronze_csv TO ROLE role_bronze_loader;

-- Write access to the audit log defined in governance.sql
GRANT USAGE ON SCHEMA brokerage_dwh.governance TO ROLE role_bronze_loader;
GRANT INSERT, SELECT ON brokerage_dwh.governance.dq_audit_log TO ROLE role_bronze_loader;

-- ---------------------------------------------------------------------------
-- 2. role_custodian — human, read-only across bronze + silver + gold
--    (the "sees everything unmasked" role — used as the unmask condition
--    in governance.sql's masking policies)
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS role_custodian;
-- GRANT ROLE role_custodian TO USER <your_human_username>;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE role_custodian;
GRANT USAGE ON DATABASE brokerage_dwh TO ROLE role_custodian;

GRANT USAGE ON SCHEMA brokerage_dwh.bronze TO ROLE role_custodian;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.bronze TO ROLE role_custodian;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.bronze TO ROLE role_custodian;

GRANT USAGE ON SCHEMA brokerage_dwh.silver TO ROLE role_custodian;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.silver TO ROLE role_custodian;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.silver TO ROLE role_custodian;
GRANT SELECT ON ALL VIEWS IN SCHEMA brokerage_dwh.silver TO ROLE role_custodian;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA brokerage_dwh.silver TO ROLE role_custodian;

GRANT USAGE ON SCHEMA brokerage_dwh.gold TO ROLE role_custodian;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.gold TO ROLE role_custodian;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.gold TO ROLE role_custodian;
GRANT SELECT ON ALL VIEWS IN SCHEMA brokerage_dwh.gold TO ROLE role_custodian;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA brokerage_dwh.gold TO ROLE role_custodian;

-- Read access to governance evidence too
GRANT USAGE ON SCHEMA brokerage_dwh.governance TO ROLE role_custodian;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.governance TO ROLE role_custodian;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.governance TO ROLE role_custodian;

-- ---------------------------------------------------------------------------
-- 3. role_analyst — human, read-only across silver + gold ONLY.
--    No bronze access. Masked on restricted_pii columns (masking policies
--    defined in governance.sql check for role_custodian/ACCOUNTADMIN to
--    unmask — role_analyst falls through to the masked branch).
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS role_analyst;
-- GRANT ROLE role_analyst TO USER <analyst_username>;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE role_analyst;
GRANT USAGE ON DATABASE brokerage_dwh TO ROLE role_analyst;

GRANT USAGE ON SCHEMA brokerage_dwh.silver TO ROLE role_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.silver TO ROLE role_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.silver TO ROLE role_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA brokerage_dwh.silver TO ROLE role_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA brokerage_dwh.silver TO ROLE role_analyst;

GRANT USAGE ON SCHEMA brokerage_dwh.gold TO ROLE role_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.gold TO ROLE role_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.gold TO ROLE role_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA brokerage_dwh.gold TO ROLE role_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA brokerage_dwh.gold TO ROLE role_analyst;

-- Explicitly no bronze access — enforced by omission, confirmed by revoke
REVOKE ALL PRIVILEGES ON SCHEMA brokerage_dwh.bronze FROM ROLE role_analyst;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA brokerage_dwh.bronze FROM ROLE role_analyst;
REVOKE ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA brokerage_dwh.bronze FROM ROLE role_analyst;

-- No access to governance schema either — that's evidence about the
-- pipeline, not something an analyst needs to see
REVOKE ALL PRIVILEGES ON SCHEMA brokerage_dwh.governance FROM ROLE role_analyst;

-- ---------------------------------------------------------------------------
-- 4. role_dbt_prod_ci — service account for dbt runs (silver/gold builds)
--    Kept separate from role_bronze_loader on purpose — see SoD note below.
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS role_dbt_prod_ci;

CREATE USER IF NOT EXISTS svc_dbt_prod_ci
    PASSWORD = '<strong-generated-secret>'   
    DEFAULT_ROLE = role_dbt_prod_ci
    DEFAULT_WAREHOUSE = compute_wh
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'CI/CD service account for dbt prod runs (silver/gold). Never a human login.';

GRANT ROLE role_dbt_prod_ci TO USER svc_dbt_prod_ci;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE role_dbt_prod_ci;
GRANT USAGE ON DATABASE brokerage_dwh TO ROLE role_dbt_prod_ci;

-- Read-only on bronze — dbt selects from bronze, never writes to it
-- (ingestion is role_bronze_loader's job, not dbt's)
GRANT USAGE ON SCHEMA brokerage_dwh.bronze TO ROLE role_dbt_prod_ci;
GRANT SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.bronze TO ROLE role_dbt_prod_ci;
GRANT SELECT ON FUTURE TABLES IN SCHEMA brokerage_dwh.bronze TO ROLE role_dbt_prod_ci;

-- Full build rights on silver/gold — this is what dbt actually does
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA brokerage_dwh.silver TO ROLE role_dbt_prod_ci;
GRANT ALL ON ALL TABLES IN SCHEMA brokerage_dwh.silver TO ROLE role_dbt_prod_ci;
GRANT ALL ON FUTURE TABLES IN SCHEMA brokerage_dwh.silver TO ROLE role_dbt_prod_ci;

GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA brokerage_dwh.gold TO ROLE role_dbt_prod_ci;
GRANT ALL ON ALL TABLES IN SCHEMA brokerage_dwh.gold TO ROLE role_dbt_prod_ci;
GRANT ALL ON FUTURE TABLES IN SCHEMA brokerage_dwh.gold TO ROLE role_dbt_prod_ci;

-- Can log dbt-side DQ results (e.g. failed dbt tests) into the same audit log
GRANT USAGE ON SCHEMA brokerage_dwh.governance TO ROLE role_dbt_prod_ci;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA brokerage_dwh.governance TO ROLE role_dbt_prod_ci;

-- Can create new schemas in the database if needed (e.g. for new dbt models)
GRANT CREATE SCHEMA ON DATABASE BROKERAGE_DWH TO ROLE ROLE_DBT_PROD_CI;
-- ---------------------------------------------------------------------------
-- Note: role_bronze_loader and role_dbt_prod_ci are kept separate
-- deliberately — Segregation of Duties. One identity that could both
-- write raw bronze AND rebuild silver/gold transformations would let a
-- single compromised credential (or a single bug) corrupt the entire
-- chain end-to-end, with no boundary in between.
-- ---------------------------------------------------------------------------