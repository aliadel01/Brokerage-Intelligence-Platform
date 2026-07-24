-- ============================================================================
-- Bronze layer DDL — brokerage-data-platform
-- Snowflake
--
-- Conventions applied throughout (see docs/ADR-* for rationale):
--   - Every table carries the standard metadata envelope:
--       _batch_id     NUMBER(9,0)         -- which load batch this row came from
--       _source_file  VARCHAR
--       _loaded_at    TIMESTAMP_NTZ(3)    -- ingestion wall-clock time
--       _row_hash     NUMBER(20,0)        -- hash of business columns, for QA/dedup checks
--   - CDC-capable sources additionally carry:
--       _cdc_flag     VARCHAR(1)          -- 'I' / 'U', backfilled to 'I' for Batch1
--       _cdc_dsn      NUMBER(20,0)        -- backfilled to 0 for Batch1
--   - No manual partitioning: Snowflake micro-partitions automatically on
--     load. CLUSTER BY is deliberately NOT applied here.
--     Add it later, per table, only if query profiles show poor pruning at
--     real data volume; premature clustering keys just cost reclustering
--     credits for no benefit on tables this size.
--  - Use the defaul compute "COMPUTE_WH"
-- ===========CR=================================================================

CREATE DATABASE IF NOT EXISTS brokerage_dwh;
CREATE SCHEMA IF NOT EXISTS brokerage_dwh.bronze;

USE SCHEMA brokerage_dwh.bronze;

-- ----------------------------------------------------------------------------
-- Staging file format + internal stage used by the ingestion loaders.
-- All loaders normalize rows into a uniform CSV shape before COPY INTO —
-- ----------------------------------------------------------------------------

CREATE FILE FORMAT IF NOT EXISTS brokerage_dwh.bronze.ff_bronze_csv
    TYPE = CSV
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('')
    EMPTY_FIELD_AS_NULL = TRUE
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
    TRIM_SPACE = TRUE
    COMPRESSION = GZIP;

CREATE STAGE IF NOT EXISTS brokerage_dwh.bronze.ingest_stage
    FILE_FORMAT = brokerage_dwh.bronze.ff_bronze_csv;

-- ============================================================================
-- ARCHETYPE A — Static reference dimensions (Batch1 only, load once)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_date
(
    SK_DateID           NUMBER(9,0),
    DateValue           DATE,
    DateDesc            VARCHAR,
    CalendarYearID      NUMBER(4,0),
    CalendarYearDesc    VARCHAR,
    CalendarQtrID       NUMBER(6,0),
    CalendarQtrDesc     VARCHAR,
    CalendarMonthID     NUMBER(6,0),
    CalendarMonthDesc   VARCHAR,
    CalendarWeekID      NUMBER(6,0),
    CalendarWeekDesc    VARCHAR,
    DayOfWeekNum        NUMBER(1,0),
    DayOfWeekDesc       VARCHAR,
    FiscalYearID        NUMBER(4,0),
    FiscalYearDesc      VARCHAR,
    FiscalQtrID         NUMBER(6,0),
    FiscalQtrDesc       VARCHAR,
    HolidayFlag         BOOLEAN,

    _batch_id           NUMBER(9,0),
    _source_file        VARCHAR,
    _loaded_at          TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash           NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_time
(
    SK_TimeID           NUMBER(9,0),
    TimeValue           VARCHAR,
    HourID               NUMBER(2,0),
    HourDesc             VARCHAR,
    MinuteID             NUMBER(2,0),
    MinuteDesc           VARCHAR,
    SecondID             NUMBER(2,0),
    SecondDesc           VARCHAR,
    MarketHoursFlag      BOOLEAN,
    OfficeHoursFlag      BOOLEAN,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_status_type
(
    ST_ID                VARCHAR(4),
    ST_NAME              VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_tax_rate
(
    TX_ID                VARCHAR(4),
    TX_NAME              VARCHAR,
    TX_RATE              NUMBER(6,5),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_industry
(
    IN_ID                VARCHAR(2),
    IN_NAME              VARCHAR,
    IN_SC_ID             VARCHAR(2),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_trade_type
(
    TT_ID                VARCHAR(3),
    TT_NAME              VARCHAR,
    TT_IS_SELL           BOOLEAN,
    TT_IS_MRKT           BOOLEAN,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- HR is technically a fact file but is Batch1-only, static, no CDC — behaves like Archetype A
CREATE TABLE IF NOT EXISTS bronze_hr
(
    EmployeeID           NUMBER(9,0),
    ManagerID            NUMBER(9,0),
    EmployeeFirstName    VARCHAR,
    EmployeeLastName     VARCHAR,
    EmployeeMI           VARCHAR(1),
    EmployeeJobCode      NUMBER(3,0),
    EmployeeBranch       VARCHAR,
    EmployeeOffice       VARCHAR,
    EmployeePhone        VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- ============================================================================
-- ARCHETYPE B — Schema-shifting CDC facts
-- Union schema: _cdc_flag/_cdc_dsn always present, backfilled ('I', 0) for
-- Batch1 rows where the source file itself carries no CDC columns. 
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_account
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    CA_ID                NUMBER(19,0),
    CA_B_ID              NUMBER(19,0),
    CA_C_ID              NUMBER(19,0),
    CA_NAME              VARCHAR,
    CA_TAX_ST            NUMBER(2,0),
    CA_ST_ID             VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_customer
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    C_ID                 NUMBER(19,0),
    C_TAX_ID             VARCHAR,
    C_ST_ID              VARCHAR(4),
    C_L_NAME             VARCHAR,
    C_F_NAME             VARCHAR,
    C_M_NAME             VARCHAR,
    C_GNDR               VARCHAR(1),
    C_TIER               NUMBER(1,0),
    C_DOB                DATE,
    C_ADLINE1            VARCHAR,
    C_ADLINE2            VARCHAR,
    C_ZIPCODE            VARCHAR,
    C_CITY               VARCHAR,
    C_STATE_PROV         VARCHAR,
    C_CTRY               VARCHAR,
    C_CTRY_1             VARCHAR,
    C_AREA_1             VARCHAR,
    C_LOCAL_1            VARCHAR,
    C_EXT_1              VARCHAR,
    C_CTRY_2             VARCHAR,
    C_AREA_2             VARCHAR,
    C_LOCAL_2            VARCHAR,
    C_EXT_2              VARCHAR,
    C_CTRY_3             VARCHAR,
    C_AREA_3             VARCHAR,
    C_LOCAL_3            VARCHAR,
    C_EXT_3              VARCHAR,
    C_PRIM_EMAIL         VARCHAR,
    C_ALT_EMAIL          VARCHAR,
    C_LCL_TX_ID          VARCHAR,
    C_NAT_TX_ID          VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_trade
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    T_ID                 NUMBER(19,0),
    T_DTS                TIMESTAMP_NTZ,
    T_ST_ID              VARCHAR(4),
    T_TT_ID              VARCHAR(3),
    T_IS_CASH            BOOLEAN,
    T_S_SYMB             VARCHAR(15),
    T_QTY                NUMBER(9,0),
    T_BID_PRICE          NUMBER(12,2),
    T_CA_ID              NUMBER(19,0),
    T_EXEC_NAME          VARCHAR,
    T_TRADE_PRICE        NUMBER(12,2),
    T_CHRG               NUMBER(12,2),
    T_COMM               NUMBER(12,2),
    T_TAX                NUMBER(12,2),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);
-- At scale, consider: ALTER TABLE bronze_trade CLUSTER BY (_batch_id); only
-- once table size and query profile justify it.

CREATE TABLE IF NOT EXISTS bronze_holding_history
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    HH_H_T_ID            NUMBER(19,0),
    HH_T_ID              NUMBER(19,0),
    HH_BEFORE_QTY        NUMBER(9,0),
    HH_AFTER_QTY         NUMBER(9,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- WatchHistory: CDC_FLAG is always 'I' per spec (rows only added, never
-- updated/deleted). _cdc_dsn kept for lineage/ordering even though every
-- row is an insert.
CREATE TABLE IF NOT EXISTS bronze_watch_history
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    W_C_ID               NUMBER(19,0),
    W_S_SYMB             VARCHAR(15),
    W_DTS                TIMESTAMP_NTZ,
    W_ACTION             VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_daily_market
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    DM_DATE              DATE,
    DM_S_SYMB            VARCHAR(15),
    DM_CLOSE             FLOAT,
    DM_HIGH              FLOAT,
    DM_LOW               FLOAT,
    DM_VOL               NUMBER(19,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- CashTransaction: CDC presence NOT confirmed against spec (see sources.md
-- note). CDC fields left nullable; _row_hash is the fallback QA/lineage
-- signal if _cdc_dsn turns out to be unreliable or absent.
CREATE TABLE IF NOT EXISTS bronze_cash_transaction
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    CT_CA_ID             NUMBER(19,0),
    CT_DTS               TIMESTAMP_NTZ,
    CT_AMT               NUMBER(15,2),
    CT_NAME              VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- ============================================================================
-- ARCHETYPE C — Full re-extract snapshot (no CDC; every batch is a complete dump)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_prospect
(
    AgencyID             VARCHAR,
    LastName             VARCHAR,
    FirstName            VARCHAR,
    MiddleInitial        VARCHAR(1),
    Gender               VARCHAR(1),
    AddressLine1         VARCHAR,
    AddressLine2         VARCHAR,
    PostalCode           VARCHAR,
    City                 VARCHAR,
    State                VARCHAR,
    Country              VARCHAR,
    Phone                VARCHAR,
    Income               NUMBER(9,0),
    NumberCars           NUMBER(2,0),
    NumberChildren       NUMBER(2,0),
    MaritalStatus        VARCHAR(1),
    Age                  NUMBER(3,0),
    CreditRating         NUMBER(4,0),
    OwnOrRentFlag        VARCHAR(1),
    Employer             VARCHAR,
    NumberCreditCards    NUMBER(2,0),
    NetWorth             NUMBER(12,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- ============================================================================
-- ARCHETYPE D — Parsed structural sources (FINWIRE, CustomerMgmt.xml)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_finwire_cmp
(
    PTS                  TIMESTAMP_NTZ,
    CompanyName          VARCHAR,
    CIK                  VARCHAR,
    Status               VARCHAR(4),
    IndustryID           VARCHAR(2),
    SPrating             VARCHAR(4),
    FoundingDate         DATE,
    AddrLine1            VARCHAR,
    AddrLine2            VARCHAR,
    PostalCode           VARCHAR,
    City                 VARCHAR,
    StateProvince        VARCHAR,
    Country              VARCHAR,
    CEOname              VARCHAR,
    Description          VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_finwire_sec
(
    PTS                  TIMESTAMP_NTZ,
    Symbol               VARCHAR(15),
    IssueType            VARCHAR(6),
    Status               VARCHAR(4),
    Name                 VARCHAR,
    ExID                 VARCHAR(6),
    ShOut                NUMBER(19,0),
    FirstTradeDate       DATE,
    FirstTradeExchg      DATE,
    Dividend             NUMBER(12,2),
    -- CoNameOrCIK resolved at parse time into two explicit, mutually
    -- exclusive columns instead of carrying the polymorphic raw field.
    CoName               VARCHAR,
    CoCIK                VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_finwire_fin
(
    PTS                  TIMESTAMP_NTZ,
    Year                 NUMBER(4,0),
    Quarter              NUMBER(1,0),
    QtrStartDate         DATE,
    PostingDate          DATE,
    Revenue              NUMBER(20,2),
    Earnings             NUMBER(20,2),
    EPS                  NUMBER(10,4),
    DilutedEPS           NUMBER(10,4),
    Margin               NUMBER(10,4),
    Inventory            NUMBER(20,2),
    Assets               NUMBER(20,2),
    Liabilities          NUMBER(20,2),
    ShOut                NUMBER(19,0),
    DilutedShOut         NUMBER(19,0),
    CoName               VARCHAR,
    CoCIK                VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_customer_mgmt_event
(
    ActionType           VARCHAR(10),
    ActionTS             TIMESTAMP_NTZ,
    C_ID                 NUMBER(19,0),
    C_TAX_ID             VARCHAR,
    C_GNDR               VARCHAR(1),
    C_TIER               NUMBER(1,0),
    C_DOB                DATE,
    C_LCL_TX_ID          VARCHAR,
    C_NAT_TX_ID          VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_customer_mgmt_account
(
    ActionTS             TIMESTAMP_NTZ,
    C_ID                 NUMBER(19,0),
    CA_ID                NUMBER(19,0),
    CA_TAX_ST            NUMBER(2,0),
    CA_B_ID              NUMBER(19,0),
    CA_NAME              VARCHAR,

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- TradeHistory: Historical Load only, per spec — no incremental counterpart.
CREATE TABLE IF NOT EXISTS bronze_trade_history
(
    TH_T_ID              NUMBER(19,0),
    TH_DTS               TIMESTAMP_NTZ,
    TH_ST_ID             VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- ============================================================================
-- Operational / control tables
-- ============================================================================

-- One row per batch, recording the vendor-supplied as-of date from
-- BatchDate.txt. Ingestion-metadata, not business data.
CREATE TABLE IF NOT EXISTS bronze_batch_control
(
    BatchID              NUMBER(9,0),
    AsOfDate             DATE,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3)
);

-- Reconciliation ground truth, sourced from *_audit.csv per component/batch.
-- Used to validate bronze row counts against vendor-reported counts.
CREATE TABLE IF NOT EXISTS bronze_source_audit
(
    DataSet              VARCHAR,
    BatchID              NUMBER(9,0),
    Date                 DATE,
    Attribute            VARCHAR,
    Value                NUMBER(19,0),
    DValue               NUMBER(20,5),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3)
);