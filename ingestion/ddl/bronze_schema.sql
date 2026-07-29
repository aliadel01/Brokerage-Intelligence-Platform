-- ============================================================================
-- Bronze layer DDL — brokerage-data-platform (TPC-DI derived)
-- Snowflake
--
-- DESIGN PRINCIPLE (governs every table below):
--   Bronze is a 1:1 mirror of the source file. If the source file carries
--   CDC_FLAG/CDC_DSN columns, bronze carries them too -- even when, on
--   inspection, the values turn out to be constant / non-informative (see
--   Archetype B "quasi-CDC" note below). Bronze NEVER drops, renames-away,
--   or infers a column the source didn't send. Any decision about how to
--   *interpret* a column (e.g. "treat this as append-only, ignore CDC_DSN
--   as an ordering signal") belongs in the SILVER layer, not here. This
--   keeps bronze replayable from the original files at all times.
--
-- Every table carries the standard metadata envelope:
--     _batch_id     NUMBER(9,0)         -- which load batch this row came from
--     _source_file  VARCHAR             -- exact filename ingested
--     _loaded_at    TIMESTAMP_NTZ(3)    -- ingestion wall-clock time
--     _row_hash     NUMBER(20,0)        -- hash of business columns, for QA/dedup
-- Sources whose file format includes CDC_FLAG/CDC_DSN (as columns in the
-- file itself) additionally carry:
--     _cdc_flag     VARCHAR(1)          -- verbatim from source; backfilled
--                                          to 'I' only for files with NO
--                                          CDC columns at all in Batch1
--                                          (Trade, HoldingHistory, WatchHistory,
--                                          DailyMarket schema-shift case)
--     _cdc_dsn      NUMBER(20,0)        -- verbatim from source; backfilled to 0
--
-- No manual partitioning / CLUSTER BY at this stage (see prior ADR notes).
-- Use the default compute warehouse "COMPUTE_WH".
-- ============================================================================

CREATE DATABASE IF NOT EXISTS brokerage_dwh;
CREATE SCHEMA IF NOT EXISTS brokerage_dwh.bronze;
USE SCHEMA brokerage_dwh.bronze;

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
-- Behavior: one-time full load; source has no CDC columns of any kind.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_date
(
    sk_dateid           NUMBER(9,0),
    datevalue           DATE,
    datedesc            VARCHAR(20),
    calendaryearid      NUMBER(4,0),
    calendaryeardesc    VARCHAR(20),
    calendarqtrid       NUMBER(6,0),
    calendarqtrdesc     VARCHAR(20),
    calendarmonthid     NUMBER(6,0),
    calendarmonthdesc   VARCHAR(20),
    calendarweekid      NUMBER(6,0),
    calendarweekdesc    VARCHAR(20),
    dayofweeknum        NUMBER(1,0),
    dayofweekdesc       VARCHAR(10),
    fiscalyearid        NUMBER(4,0),
    fiscalyeardesc      VARCHAR(20),
    fiscalqtrid         NUMBER(6,0),
    fiscalqtrdesc       VARCHAR(20),
    holidayflag         BOOLEAN,

    _batch_id           NUMBER(9,0),
    _source_file        VARCHAR,
    _loaded_at          TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash           NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_time
(
    sk_timeid           NUMBER(9,0),
    timevalue           VARCHAR(20),
    hourid              NUMBER(2,0),
    hourdesc            VARCHAR(20),
    minuteid            NUMBER(2,0),
    minutedesc          VARCHAR(20),
    secondid            NUMBER(2,0),
    seconddesc          VARCHAR(20),
    markethoursflag     BOOLEAN,
    officehoursflag     BOOLEAN,

    _batch_id           NUMBER(9,0),
    _source_file        VARCHAR,
    _loaded_at          TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash           NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_status_type
(
    st_id                VARCHAR(4),
    st_name              VARCHAR(10),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_tax_rate
(
    tx_id                VARCHAR(4),
    tx_name              VARCHAR(50),
    tx_rate              NUMBER(6,5),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_industry
(
    in_id                VARCHAR(2),
    in_name              VARCHAR(50),
    in_sc_id             VARCHAR(2),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- NOTE: TT_IS_SELL / TT_IS_MRKT are documented as NUM(1) flags in the spec
-- (0/1), not a native BOOLEAN in the source file. Kept as NUMBER here to
-- stay 1:1 with the source; casting to BOOLEAN is a silver decision.
CREATE TABLE IF NOT EXISTS bronze_trade_type
(
    tt_id                VARCHAR(3),
    tt_name              VARCHAR(12),
    tt_is_sell           NUMBER(1,0),
    tt_is_mrkt           NUMBER(1,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_hr
(
    employeeid           NUMBER(9,0),
    managerid            NUMBER(9,0),
    employeefirstname    VARCHAR(30),
    employeelastname     VARCHAR(30),
    employeemi           VARCHAR(1),
    employeejobcode      NUMBER(3,0),
    employeebranch       VARCHAR(30),
    employeeoffice       VARCHAR(10),
    employeephone        VARCHAR(14),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);


-- ============================================================================
-- ARCHETYPE B — Schema-shifting CDC facts
-- Behavior: Batch1 (historical) has NO CDC_FLAG/CDC_DSN columns at all --
-- the file's field count is genuinely smaller. Batch2/3 (incremental)
-- prepend CDC_FLAG/CDC_DSN. Bronze backfills ('I', 0) for Batch1 rows so
-- the union schema is queryable, per source spec confirmation:
--   "The CDC_FLAG and CDC_DSN fields are not present in the data set used
--    by the Historical Load."
--
-- account / customer / trade: REAL CDC. CDC_FLAG genuinely varies I/U and reflects
-- an actual update to an existing entity's attributes.
--
-- holding_history / watch_history / daily_market / cash_transaction:
-- QUASI-CDC. The file format carries CDC_FLAG/CDC_DSN, but in the actual
-- data CDC_FLAG is observed to be constant ('I') for all of them. 
-- Bronze still stores these columns verbatim (1:1 principle) 
-- the decision to treat them as append-only events (not "latest state wins") is made in SILVER, not here.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_account
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    ca_id                NUMBER(19,0),
    ca_b_id              NUMBER(19,0),
    ca_c_id              NUMBER(19,0),
    ca_name              VARCHAR(50),
    ca_tax_st            NUMBER(2,0),
    ca_st_id             VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_customer
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    c_id                 NUMBER(19,0),
    c_tax_id             VARCHAR(20),
    c_st_id              VARCHAR(4),
    c_l_name             VARCHAR(25),
    c_f_name             VARCHAR(20),
    c_m_name             VARCHAR(1),
    c_gndr               VARCHAR(1),
    c_tier               NUMBER(1,0),
    c_dob                DATE,
    c_adline1            VARCHAR(80),
    c_adline2            VARCHAR(80),
    c_zipcode            VARCHAR(12),
    c_city               VARCHAR(25),
    c_state_prov         VARCHAR(20),
    c_ctry               VARCHAR(24),
    c_ctry_1             VARCHAR(3),
    c_area_1             VARCHAR(3),
    c_local_1            VARCHAR(10),
    c_ext_1              VARCHAR(5),
    c_ctry_2             VARCHAR(3),
    c_area_2             VARCHAR(3),
    c_local_2            VARCHAR(10),
    c_ext_2              VARCHAR(5),
    c_ctry_3             VARCHAR(3),
    c_area_3             VARCHAR(3),
    c_local_3            VARCHAR(10),
    c_ext_3              VARCHAR(5),
    c_prim_email         VARCHAR(50),
    c_alt_email          VARCHAR(50),
    c_lcl_tx_id          VARCHAR(4),
    c_nat_tx_id          VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_trade
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    t_id                 NUMBER(19,0),
    t_dts                TIMESTAMP_NTZ,
    t_st_id              VARCHAR(4),
    t_tt_id              VARCHAR(3),
    t_is_cash            BOOLEAN,
    t_s_symb             VARCHAR(15),
    t_qty                NUMBER(9,0),
    t_bid_price          NUMBER(12,2),
    t_ca_id              NUMBER(19,0),
    t_exec_name          VARCHAR(50),
    t_trade_price        NUMBER(12,2),
    t_chrg               NUMBER(12,2),
    t_comm               NUMBER(12,2),
    t_tax                NUMBER(12,2),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_holding_history
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    hh_h_t_id            NUMBER(19,0),
    hh_t_id              NUMBER(19,0),
    hh_before_qty        NUMBER(9,0),
    hh_after_qty         NUMBER(9,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_watch_history
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    w_c_id               NUMBER(19,0),
    w_s_symb             VARCHAR(15),
    w_dts                TIMESTAMP_NTZ,
    w_action             VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_daily_market
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    dm_date              DATE,
    dm_s_symb            VARCHAR(15),
    dm_close             FLOAT,
    dm_high              FLOAT,
    dm_low               FLOAT,
    dm_vol               NUMBER(19,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);


CREATE TABLE IF NOT EXISTS bronze_cash_transaction
(
    _cdc_flag            VARCHAR(1),
    _cdc_dsn             NUMBER(20,0),

    ct_ca_id             NUMBER(19,0),
    ct_dts               TIMESTAMP_NTZ,
    ct_amt               NUMBER(15,2),
    ct_name              VARCHAR(100),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);


-- ============================================================================
-- ARCHETYPE C — Full re-extract snapshot (no CDC; every batch is a
-- complete dump of the entire entity, not a delta)
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_prospect
(
    agencyid             VARCHAR(30),
    lastname             VARCHAR(30),
    firstname            VARCHAR(30),
    middleinitial        VARCHAR(1),
    gender               VARCHAR(1),
    addressline1         VARCHAR(80),
    addressline2         VARCHAR(80),
    postalcode           VARCHAR(12),
    city                 VARCHAR(25),
    state                VARCHAR(20),
    country              VARCHAR(24),
    phone                VARCHAR(30),
    income               NUMBER(9,0),
    numbercars           NUMBER(2,0),
    numberchildren       NUMBER(2,0),
    maritalstatus        VARCHAR(1),
    age                  NUMBER(3,0),
    creditrating         NUMBER(4,0),
    ownorrentflag        VARCHAR(1),
    employer             VARCHAR(30),
    numbercreditcards    NUMBER(2,0),
    networth             NUMBER(12,0),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);


-- ============================================================================
-- ARCHETYPE D — Parsed structural sources (FINWIRE fixed-width, CustomerMgmt.xml)
-- Behavior: no CDC columns in the raw file at all. FINWIRE is Batch1-only,
-- append-only-by-PTS. CustomerMgmt.xml is Batch1 (Historical Load) only,
-- and its ActionType attribute carries richer state-transition information
-- (NEW/ADDACCT/UPDCUST/UPDACCT/CLOSEACCT/INACT) than the simple I/U used
-- by Account.txt/Customer.txt from Batch2/3 onward. Bronze keeps ActionType
-- verbatim as its own column; unifying it with Account.txt/Customer.txt's
-- CDC_FLAG vocabulary is a SILVER decision, not a bronze one -- the two
-- vocabularies are not a 1:1 mapping (see docs/silver_customer_account_unification.md
-- for the open questions on CLOSEACCT/INACT status inference).
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_finwire_cmp
(
    pts                  TIMESTAMP_NTZ,
    companyname          VARCHAR(60),
    cik                  VARCHAR(10),
    status               VARCHAR(4),
    industryid           VARCHAR(2),
    sprating             VARCHAR(4),
    foundingdate         DATE,
    addrline1            VARCHAR(80),
    addrline2            VARCHAR(80),
    postalcode           VARCHAR(12),
    city                 VARCHAR(25),
    stateprovince        VARCHAR(20),
    country              VARCHAR(24),
    ceoname              VARCHAR(46),
    description          VARCHAR(150),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_finwire_sec
(
    pts                  TIMESTAMP_NTZ,
    symbol               VARCHAR(15),
    issuetype            VARCHAR(6),
    status               VARCHAR(4),
    name                 VARCHAR(70),
    exid                 VARCHAR(6),
    shout                NUMBER(19,0),
    firsttradedate       DATE,
    firsttradeexchg      DATE,
    dividend             NUMBER(12,2),
    -- CoNameOrCIK resolved at parse time into two explicit, mutually
    -- exclusive columns instead of carrying the raw polymorphic field.
    coname               VARCHAR(60),
    cocik                VARCHAR(10),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

CREATE TABLE IF NOT EXISTS bronze_finwire_fin
(
    pts                  TIMESTAMP_NTZ,
    year                 NUMBER(4,0),
    quarter              NUMBER(1,0),
    qtrstartdate         DATE,
    postingdate          DATE,
    revenue              NUMBER(20,2),
    earnings             NUMBER(20,2),
    eps                  NUMBER(10,4),
    dilutedeps           NUMBER(10,4),
    margin               NUMBER(10,4),
    inventory            NUMBER(20,2),
    assets               NUMBER(20,2),
    liabilities          NUMBER(20,2),
    shout                NUMBER(19,0),
    dilutedshout         NUMBER(19,0),
    coname               VARCHAR(60),
    cocik                VARCHAR(10),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- bronze_mgmt_customer: flattened from CustomerMgmt.xml <Action>/<Customer>.
-- One row per Action that has a Customer element (i.e. every action type).
-- Per spec, UPDCUST sends only C_ID plus the fields that changed -- all
-- other columns are NULL on that row (sparse payload). Reconciling that
-- sparseness into a "current full record" (e.g. via COALESCE/carry-forward)
-- is explicitly a SILVER responsibility, not bronze's.
CREATE TABLE IF NOT EXISTS bronze_mgmt_customer
(
    actiontype           VARCHAR(9),
    actionts             TIMESTAMP_NTZ,
    c_id                 NUMBER(19,0),
    c_tax_id             VARCHAR(20),
    c_gndr               VARCHAR(1),
    c_tier               NUMBER(1,0),
    c_dob                DATE,
    c_l_name             VARCHAR(25),
    c_f_name             VARCHAR(20),
    c_m_name             VARCHAR(1),
    c_adline1            VARCHAR(80),
    c_adline2            VARCHAR(80),
    c_zipcode            VARCHAR(12),
    c_city               VARCHAR(25),
    c_state_prov         VARCHAR(20),
    c_ctry               VARCHAR(24),
    c_prim_email         VARCHAR(50),
    c_alt_email          VARCHAR(50),
    c_ctry_1             VARCHAR(3),
    c_area_1             VARCHAR(3),
    c_local_1            VARCHAR(10),
    c_ext_1              VARCHAR(5),
    c_ctry_2             VARCHAR(3),
    c_area_2             VARCHAR(3),
    c_local_2            VARCHAR(10),
    c_ext_2              VARCHAR(5),
    c_ctry_3             VARCHAR(3),
    c_area_3             VARCHAR(3),
    c_local_3            VARCHAR(10),
    c_ext_3              VARCHAR(5),
    c_lcl_tx_id          VARCHAR(4),
    c_nat_tx_id          VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);

-- bronze_mgmt_account: flattened from CustomerMgmt.xml <Action>/<Customer>/<Account>.
-- One row per Account element nested inside an Action (zero or more per
-- Action -- NEW/ADDACCT can carry several accounts in one Action). CA_ID is
-- required on NEW/ADDACCT/UPDACCT/CLOSEACCT; CLOSEACCT/INACT actions carry
-- only identifiers with no attribute payload (see docs note on status
-- inference -- CLOSEACCT/INACT status is NOT present in source data at all,
-- it must be derived downstream from ActionType itself).
CREATE TABLE IF NOT EXISTS bronze_mgmt_account
(
    actiontype           VARCHAR(9),
    actionts             TIMESTAMP_NTZ,
    c_id                 NUMBER(19,0),
    ca_id                NUMBER(19,0),
    ca_tax_st            NUMBER(1,0),
    ca_b_id              NUMBER(19,0),
    ca_name              VARCHAR(50),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);


-- ============================================================================
-- ARCHETYPE E — Historical-load-only fact 
-- Behavior: loads once, in Batch1 only, like Archetype A -- but unlike A,
-- this is trade-lifecycle FACT data (linked to bronze_trade via T_ID), not
-- static reference/dimension data. No Batch2/3 counterpart exists at all
-- per spec: "This file is used only in the Historical Load."
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_trade_history
(
    th_t_id              NUMBER(19,0),
    th_dts               TIMESTAMP_NTZ,
    th_st_id             VARCHAR(4),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3),
    _row_hash            NUMBER(20,0)
);


-- ============================================================================
-- Operational / Control tables — NOT business data, NOT classified under
-- A/B/C/D/E. These describe the pipeline's own execution (what batch ran,
-- what date it represents, vendor-supplied reconciliation counts). Loaded
-- every batch, but via merge/upsert-style logic since each is keyed and
-- small, not full-refresh or CDC in the business-data sense.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze_batch_control
(
    batchid              NUMBER(9,0),
    asofdate             DATE,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3)
);

CREATE TABLE IF NOT EXISTS bronze_source_audit
(
    dataset              VARCHAR(20),
    batchid              NUMBER(5,0),
    date                 DATE,
    attribute            VARCHAR(50),
    value                NUMBER(15,0),
    dvalue               NUMBER(20,5),

    _batch_id            NUMBER(9,0),
    _source_file         VARCHAR,
    _loaded_at           TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(3)
);