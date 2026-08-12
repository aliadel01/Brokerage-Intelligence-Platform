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

-- ---------------------------------------------------------------------------
-- DBT test results log — structured record of all dbt test results, including pass/fail/warn/error.
-- ---------------------------------------------------------------------------

create table if not exists governance.dbt_test_results (
    result_id       number(38,0) autoincrement primary key,
    invocation_id   varchar         not null,  -- dbt's UUID for this run, groups all tests from one `dbt build`/`dbt test`
    run_started_at  timestamp_ntz(3) not null,
    test_name       varchar         not null,  -- unique_id, e.g. unique_silver_trade_trade_id
    model_name      varchar,                   -- model the test is attached to, e.g. silver_trade
    status          varchar(10)     not null,  -- pass / fail / warn / error / skipped
    severity        varchar(10)     not null,  -- error / warn (from test's own config)
    failures        number(38,0),              -- row count that failed the test, null if not applicable
    execution_time  float,                     -- seconds
    message         varchar,                   -- dbt's own failure message
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3)
    );

-- ---------------------------------------------------------------------------
-- Erasure log — structured record of all erasure requests and their status.
-- ---------------------------------------------------------------------------

CREATE TABLE governance.erasure_log (
    erasure_id      NUMBER AUTOINCREMENT,
    customer_id     NUMBER NOT NULL,
    requested_at    TIMESTAMP_NTZ NOT NULL,
    erased_at       TIMESTAMP_NTZ,
    reason          VARCHAR(500),
    status          VARCHAR(50) NOT NULL,
    affected_layers VARCHAR(500),
    requested_by    VARCHAR(255),
    notes           VARCHAR(1000),

    CONSTRAINT pk_erasure_log PRIMARY KEY (erasure_id)
);