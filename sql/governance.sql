-- ============================================================================
-- Governance objects — brokerage-data-platform
-- Evidence about the pipeline (DQ audit trail) and control definitions
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS brokerage_dwh.governance;

-- ---------------------------------------------------------------------------
-- DQ audit log — structured record of reconciliation mismatches and other
-- DQ issues, replacing print()-only warnings with a queryable trail.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS brokerage_dwh.governance.dq_audit_log
(
    log_id          NUMBER AUTOINCREMENT,
    _batch_id       NUMBER(9,0),
    check_type      VARCHAR(50),   -- e.g. 'reconciliation_mismatch', 'dbt_test_failure'
    source_file     VARCHAR,
    expected_value  VARCHAR,       -- kept as string; expected/actual can be counts or other types
    actual_value    VARCHAR,
    severity        VARCHAR(20),   -- 'WARNING' / 'ERROR'
    message         VARCHAR,
    logged_at       TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3)
);