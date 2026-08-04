# Silver Layer

## Table of contents
- [Silver Layer](#silver-layer)
  - [Table of contents](#table-of-contents)
  - [Governing principle](#governing-principle)
  - [Strategies by archetype](#strategies-by-archetype)
  - [Model classification table](#model-classification-table)
  - [Architecture Decision Records](#architecture-decision-records)
    - [ADR-001: Account/Customer](#adr-001-accountcustomer)
    - [ADR-002: Trade](#adr-002-trade)
    - [ADR-003: Two dedup shapes, one macro](#adr-003-two-dedup-shapes-one-macro)
    - [ADR-004: Custom `generate_schema_name` macro](#adr-004-custom-generate_schema_name-macro)

## Governing principle

Silver answers "what does it mean." Bronze answers "what did the source
send." Every interpretive decision the bronze doc leaves open — unifying
two vocabularies, choosing append-only vs. state-tracking, deriving a
value the source never sent — gets resolved here, not in bronze.

Silver models clean, cast, dedup, and (where needed) historize data. They
do not touch the raw envelope columns beyond carrying `_batch_id` forward
for lineage.

Two dedup shapes are used across this layer. The choice depends on the
source type, not on habit. Both use the same `dedup_latest(relation,
partition_by, order_by)` macro — never an inline `qualify`.

- **State-tracking dedup** (static dimensions, Prospect, Trade): one row
  per business key, latest wins.
- **Append-only dedup** (quasi-CDC event logs; the safety pass inside
  `silver_account`/`silver_customer`; and the final duplicate-ingestion
  pass inside `silver_trade_history`): removes exact duplicate rows
  only. Every distinct event stays.

## Strategies by archetype

| Archetype | Sources | Strategy |
|---|---|---|
| A. Static/reference dimensions | Date, Time, StatusType, TaxRate, Industry, TradeType, HR | Pass-through + defensive dedup. No CDC, no history. |
| B. Real CDC | Account, Customer | SCD2, batch-level grain. Account/Customer unify a flat-file source and an XML source. |
| B. Real CDC (latest-state) | Trade | State-tracking dedup, one row per `trade_id`, latest wins — same shape as Prospect. Status-transition history is owned separately by `silver_trade_history` (see ADR-002). |
| B. Quasi-CDC | HoldingHistory, WatchHistory, DailyMarket, CashTransaction | Append-only event log. `_cdc_flag`/`_cdc_dsn` kept for lineage only, never used for "latest wins". |
| C. Snapshot dimension | Prospect | SCD1, one row per `agency_id`, latest batch wins. |
| D. FINWIRE | CMP, SEC, FIN | Append-only-by-PTS, Batch1 only. |
| D. CustomerMgmt.xml | mgmt_customer, mgmt_account | Not modeled on their own. Folded into `silver_account`/`silver_customer`. |
| E. Batch1-only fact (bronze), cross-batch model (silver) | TradeHistory | Bronze source is Batch1-only. The silver model, `silver_trade_history`, unions it with Batch2/3 status transitions derived from `bronze_trade`, covering the full trade lifecycle across every batch (see ADR-002). |

## Model classification table

| # | Bronze source(s) | Silver model | Key | Status |
|---|---|---|---|---|
| 1 | bronze_date | `silver_date` | `date_sk` | built |
| 2 | bronze_time | `silver_time` | `time_sk` | built |
| 3 | bronze_status_type | `silver_status_type` | `status_id` | built |
| 4 | bronze_tax_rate | `silver_tax_rate` | `tax_rate_id` | built |
| 5 | bronze_industry | `silver_industry` | `industry_id` | built |
| 6 | bronze_trade_type | `silver_trade_type` | `trade_type_id` | built |
| 7 | bronze_hr | `silver_hr` | `employee_id` | built |
| 8 | bronze_prospect | `silver_prospect` | `agency_id` | built |
| 9 | bronze_account + bronze_mgmt_account | `silver_account` | `account_id` per version | built |
| 10 | bronze_customer + bronze_mgmt_customer | `silver_customer` | `customer_id` per version | built |
| 11 | bronze_trade | `silver_trade` | `trade_id` (latest state) | built |
| 12 | bronze_trade_history + bronze_trade (Batch2/3 only) | `silver_trade_history` | `trade_id, status_ts, status_id` | built |
| 13 | bronze_holding_history | `silver_holding_history` | `_row_hash` | built |
| 14 | bronze_watch_history | `silver_watch_history` | `_row_hash` | built |
| 15 | bronze_daily_market | `silver_daily_market` | `_row_hash` | built |
| 16 | bronze_cash_transaction | `silver_cash_transaction` | `_row_hash` | built |
| 17 | bronze_finwire_cmp | `silver_finwire_company` | `cik, posting_ts` | built |
| 18 | bronze_finwire_sec | `silver_finwire_security` | `security_symbol, posting_ts` | built |
| 19 | bronze_finwire_fin | `silver_finwire_financials` | `coalesce(cik, name), year, quarter` | built |
| — | bronze_batch_control, bronze_source_audit | — | operational, no silver model planned | — |

## Architecture Decision Records

### ADR-001: Account/Customer
**Status:** Accepted
**Decision:**
- **Sources unified:** flat-file CDC (`bronze_account`/`bronze_customer`)
  + XML (`bronze_mgmt_account`/`bronze_mgmt_customer`).
- **Grain:** one row per entity per `_batch_id`. Keep the last event
  inside that batch, and only if it differs from the previous kept
  batch's version on tracked columns. *Reason: these entities change
  slowly enough that daily precision is enough for the business.*
- **XML actiontype → cdc_flag:** Account: `NEW→I, ADDACCT→U,
  CLOSEACCT→U, UPDACCT→U`. Customer: `NEW→I, UPDCUST→U, INACT→U`.
  *Reason: these values don't exist as real `actiontype`s — the true
  status lives in `ca_st_id`/`c_st_id` on the flat-file source, which
  already carries a real `_cdc_flag`.*
- **XML status derivation:** Account: `NEW/ADDACCT→'ACTV', UPDACCT→NULL`
  (forward-filled), `CLOSEACCT→'INAC'`. Customer: `NEW→'ACTV',
  UPDCUST→NULL` (forward-filled), `INACT→'INAC'`. *Reason: reuses the
  flat-file's own `ACTV`/`INAC` domain per the TPC-DI spec's definition
  of closing/deactivating; `UPDACCT`/`UPDCUST` carry no status signal at
  all (confirmed), so they forward-fill.*
- **Customer actiontype filter:** `bronze_mgmt_customer` kept only where
  `actiontype IN ('NEW', 'UPDCUST', 'INACT')`. *Reason: a real query
  found six actiontypes present; `ADDACCT`/`CLOSEACCT`/`UPDACCT` carry
  zero customer attributes (account-scoped events in the same flattened
  stream) and stay excluded. `INACT` carries zero attributes too, but per
  spec it's a real customer state signal ("has become inactive"), so it's
  included.*
- **Tracked vs. carried-only:** only tracked columns create a new SCD2
  version. Account tracked: `status_id, account_name`; carried-only:
  `broker_id, tax_status`. Customer tracked: `status_id, last_name,
  first_name, tier, address_line1, city, state_province, country,
  primary_email`; carried-only: `middle_name, gender, date_of_birth,
  address_line2, postal_code, alternate_email, tax_id,
  local_tax_rate_id, national_tax_rate_id`, all 3 phone numbers.
  *Reason: the business needs history for identity/location/status, not
  for fields like phone numbers (delegated business call).*
- **Forward-fill:** `LAST_VALUE(...) IGNORE NULLS`, run over every
  individual event first (full chronological order) before the batch
  collapses to its last row. *Reason: XML actions (e.g. `UPDACCT`) can
  send a partial payload. Collapsing before forward-filling was tried
  and caused a bug — an early event's value could miss the later, kept
  row.*
- **Ordering key:** `_batch_id, action_ts, _cdc_dsn, _loaded_at` for both
  dedup and forward-fill. *Reason: the XML historical batch shares one
  `_batch_id` and a fixed `_cdc_dsn = 0` across several events for the
  same entity — without `action_ts`, those events tie and sort
  undefined. Safe only because a batch never mixes flat-file and XML
  rows (confirmed).*

### ADR-002: Trade
**Status:** Accepted
**Decision:** Trade is split across two models, by concern.
`bronze_trade` (Batch2/3) naturally arrives as one CDC event per status
change — multiple rows per `trade_id`, the same shape Account/Customer's
CDC sources have. Trade deliberately does NOT keep that natural event
shape in a single model, unlike Account/Customer:
- **`silver_trade`:** one row per `trade_id` — the latest known state.
  State-tracking dedup, same shape as `silver_prospect`. This
  deliberately collapses `bronze_trade`'s natural multi-row-per-trade
  CDC shape down to one row. No tracked/carried-only distinction and no
  forward-fill apply here — every column is simply "latest known value",
  and each event was confirmed to always send a full payload (no gap to
  fill).
- **`silver_trade_history`:** one row per `(trade_id, status_ts,
  status_id)` — the full status-transition history, unifying
  `bronze_trade_history` (Batch1's complete lifecycle) with
  `bronze_trade` filtered to Batch2/3 and to real status transitions
  only (a CDC event re-sending the same status is not a new row).
  `bronze_trade`'s Batch1 rows are excluded from this union entirely —
  forced, not a judgment call: a real query confirmed `TRADE_ID` is
  unique in Batch1 within `bronze_trade`, meaning it carries only each
  trade's *final* status for Batch1, not its lifecycle. Unioning it in
  would either exactly duplicate or silently conflict with the
  already-complete Batch1 lifecycle `bronze_trade_history` already
  provides, and neither source alone holds the complete Batch1 picture
  without the other. A `_source_model` column
  (`'bronze_trade_history'` vs `'bronze_trade'`) records which bronze
  source produced each row, rather than relying on `_batch_id = 1` as an
  implicit proxy.
**Reason:** The gold-layer target is a Kimball star schema. A
`fact_trade` built directly on `bronze_trade`'s natural
event-per-status-change shape would force every downstream aggregate
(`SUM(commission)`, etc.) to filter to a single current version, or
silently multiply those measures by however many status transitions the
trade went through — a classic Kimball fan trap. Splitting "current
economic state" (`silver_trade`) from "status journey"
(`silver_trade_history`) into two models removes that failure mode
structurally instead of relying on query discipline.

### ADR-003: Two dedup shapes, one macro
**Status:** Accepted
**Decision:** All dedup goes through `dedup_latest`. Static dimensions,
Prospect, and Trade (`silver_trade`) use state-tracking dedup (one row
per key, latest wins). Quasi-CDC event logs, the SCD2 models
(Account/Customer), and `silver_trade_history` use duplicate-removal
dedup (keep every distinct event/version).
**Reason:** These two source shapes need different logic. One macro keeps
the logic in one place instead of scattered inline `qualify` clauses.
Trade sits in the state-tracking bucket despite its bronze source having
real CDC columns (`_cdc_flag`/`_cdc_dsn`) — see ADR-002: `silver_trade`
is deliberately latest-state only, not versioned, so it needs the same
shape as a snapshot dimension, not the SCD2 shape its bronze archetype
would otherwise suggest.

### ADR-004: Custom `generate_schema_name` macro
**Status:** Accepted
**Decision:** Override `generate_schema_name` so a model's `+schema:`
config is used exactly as given, instead of dbt's default
`<target_schema>_<custom_schema>` prefix.
**Reason:** Standard, well-known dbt pattern. Not specific to this data
model.