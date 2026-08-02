# Silver Layer

## Table of contents
- [Governing principle](#governing-principle)
- [Strategies by archetype](#strategies-by-archetype)
- [Model classification table](#model-classification-table)
- [Architecture Decision Records](#architecture-decision-records)
- [Open questions](#open-questions)

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

- **State-tracking dedup** (static dimensions, Prospect): one row per
  business key, latest wins.
- **Append-only dedup** (quasi-CDC event logs, and the safety pass inside
  `silver_account`/`silver_customer`/`silver_trade`): removes exact
  duplicate rows only. Every distinct event stays.

## Strategies by archetype

| Archetype | Sources | Strategy |
|---|---|---|
| A. Static/reference dimensions | Date, Time, StatusType, TaxRate, Industry, TradeType, HR | Pass-through + defensive dedup. No CDC, no history. |
| B. Real CDC | Account, Customer, Trade | SCD2. Account/Customer unify a flat-file source and an XML source. |
| B. Quasi-CDC | HoldingHistory, WatchHistory, DailyMarket, CashTransaction | Append-only event log. `_cdc_flag`/`_cdc_dsn` kept for lineage only, never used for "latest wins". |
| C. Snapshot dimension | Prospect | SCD1, one row per `agency_id`, latest batch wins. |
| D. FINWIRE | CMP, SEC, FIN | Append-only-by-PTS, Batch1 only. |
| D. CustomerMgmt.xml | mgmt_customer, mgmt_account | Not modeled on their own. Folded into `silver_account`/`silver_customer`. |
| E. Batch1-only fact | TradeHistory | Pass-through + defensive dedup. Key is `(trade_id, status_ts, status_id)`. |

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
| 11 | bronze_trade | `silver_trade` | `trade_id` per event | built |
| 12 | bronze_trade_history | `silver_trade_history` | `trade_id, status_ts, status_id` | built |
| 13 | bronze_holding_history | `silver_holding_history` | `_row_hash` | built |
| 14 | bronze_watch_history | `silver_watch_history` | `_row_hash` | built |
| 15 | bronze_daily_market | `silver_daily_market` | `_row_hash` | built |
| 16 | bronze_cash_transaction | `silver_cash_transaction` | `_row_hash` | built |
| 17 | bronze_finwire_cmp | `silver_finwire_company` | `cik, posting_ts` | built |
| 18 | bronze_finwire_sec | `silver_finwire_security` | `security_symbol, posting_ts` | built |
| 19 | bronze_finwire_fin | `silver_finwire_financials` | `coalesce(cik, name), year, quarter` | built |
| — | bronze_batch_control, bronze_source_audit | — | operational, no silver model planned | — |

## Architecture Decision Records

### ADR-001: Two dedup shapes, one macro
**Status:** Accepted
**Decision:** All dedup goes through `dedup_latest`. Static dimensions and
Prospect use state-tracking dedup (one row per key). Quasi-CDC and the
real-CDC models use duplicate-removal dedup (keep every distinct event).
**Reason:** These two source shapes need different logic. One macro keeps
the logic in one place instead of scattered inline `qualify` clauses.

### ADR-002: Account/Customer CDC flag mapping
**Status:** Accepted (confirmed against real data)
**Decision:**
- Account XML actiontype → cdc_flag: `NEW → I`, `ADDACCT → U`,
  `CLOSEACCT → U`, `UPDACCT → U`.
- Customer XML actiontype → cdc_flag: `NEW → I`, `UPDCUST → U`,
  `INACT → U`.
**Rejected alternative:** An earlier draft assumed `ACTV`/`INAC` were
actiontype values that needed a mapping rule. A real query against
`bronze_mgmt_account` showed these values do not exist there at all —
they are `ca_st_id`/`c_st_id` values in the flat-file source, which
already carries a real `_cdc_flag` and needs no derivation. That draft
rule is dropped.

### ADR-003: Status derivation for XML rows
**Status:** Accepted
**Decision:**
- Account: `NEW/ADDACCT → 'ACTV'`, `UPDACCT → NULL` (forward-filled),
  `CLOSEACCT → 'INAC'`.
- Customer: `NEW → 'ACTV'`, `UPDCUST → NULL` (forward-filled),
  `INACT → 'INAC'`.
**Reason:** `CLOSEACCT`/`INACT` are defined in the TPC-DI spec as closing
an account / making a customer inactive. The real flat-file status domain
observed is only `ACTV`/`INAC`, so these reuse that same code instead of
inventing a new one. `UPDACCT`/`UPDCUST` carry no status signal at all —
confirmed by a field-completeness check — so they forward-fill instead.

### ADR-004: Customer actiontype filter
**Status:** Accepted (supersedes an earlier draft)
**Decision:** `bronze_mgmt_customer` rows are kept only where
`actiontype IN ('NEW', 'UPDCUST', 'INACT')`.
**Rejected alternative:** An earlier draft filtered to `NEW`/`UPDCUST`
only, assuming those were the only two actiontypes present. A real query
found six actiontypes in `bronze_mgmt_customer`: `NEW, ADDACCT,
CLOSEACCT, UPDCUST, INACT, UPDACCT`. `ADDACCT`/`CLOSEACCT`/`UPDACCT` carry
zero customer attributes (only `C_ID`) and stay excluded — they are
account-scoped events in the same flattened stream. `INACT` also carries
zero attributes, but per the TPC-DI spec it means "an existing customer
has become inactive" — a real customer state signal despite the empty
payload. It is included.

### ADR-005: SCD2 grain
**Status:** Accepted (supersedes an earlier draft)
**Decision:**
- `silver_account`/`silver_customer`: one row per entity per `_batch_id`.
  Keep the last event inside that batch. Keep that row only if it differs
  from the previous kept batch's version, on tracked columns.
- `silver_trade`: one row per `trade_id` per event, using
  `trade_timestamp` as the boundary, not `_batch_id`. Keep a row only if
  it differs from the immediately preceding event, on tracked columns.
**Reason:** Account/Customer change slowly enough that daily (batch)
precision is enough for the business. Trade can move through several
statuses inside one batch (submitted, then completed, same day), so
batch-level grain would hide real same-day changes.
**Rejected alternative:** Event-level grain for Account/Customer too (one
row per real change, even inside the same batch). This was tried and
reverted — the business does not need intraday history for these two
entities, and batch-level grain is simpler.
**Implementation note:** Forward-fill must run over every individual
event first, in full chronological order, before the batch collapses to
its last row. Collapsing first was tried and caused a bug: an early event
in a batch could carry a value that never reached the later, kept row.

### ADR-006: Tracked vs. carried-only columns
**Status:** Accepted (delegated decision — business call, not a data
question)
**Decision:** Only "tracked" columns create a new SCD2 version. Other
columns are forward-filled and shown on every row, but changing them
alone does not create a new version.
- `silver_account` tracked: `status_id, account_name`. Carried-only:
  `broker_id, tax_status`.
- `silver_customer` tracked: `status_id, last_name, first_name, tier,
  address_line1, city, state_province, country, primary_email`.
  Carried-only: `middle_name, gender, date_of_birth, address_line2,
  postal_code, alternate_email, tax_id, local_tax_rate_id,
  national_tax_rate_id`, all 3 phone numbers.
- `silver_trade` tracked: `status_id` only. Carried-only: `trade_type_id,
  is_cash, symbol, quantity, bid_price, customer_account_id,
  execution_name, trade_price, charge, commission, tax`.
**Reason:** The business needs history for identity/location/status
fields (example: where a customer used to live). It does not need history
for fields like phone numbers.

### ADR-007: Forward-fill only where the source is sparse
**Status:** Accepted
**Decision:** `silver_account`/`silver_customer` use
`LAST_VALUE(...) IGNORE NULLS` forward-fill. `silver_trade` does not.
**Reason:** Account/Customer XML actions can send a partial payload
(example: `UPDACCT` sends no status at all). Trade events were confirmed
to always send the full payload, so there is no gap to fill.

### ADR-008: Ordering key for Account/Customer
**Status:** Accepted
**Decision:** Both dedup and forward-fill order by `_batch_id, action_ts,
_cdc_dsn, _loaded_at` — not `_batch_id, _cdc_dsn, _loaded_at` alone.
**Reason:** The XML historical batch shares one `_batch_id` and a fixed
`_cdc_dsn = 0` across several events for the same entity. Without
`action_ts`, those events would tie and sort in an undefined order.
`action_ts` is null on flat-file rows and populated on XML rows. This is
safe only because a batch never mixes both sources — confirmed.

### ADR-009: Custom `generate_schema_name` macro
**Status:** Accepted
**Decision:** Override `generate_schema_name` so a model's `+schema:`
config is used exactly as given, instead of dbt's default
`<target_schema>_<custom_schema>` prefix.
**Reason:** Standard, well-known dbt pattern. Not specific to this data
model.

## Open questions

1. Official Prospect-to-Customer matching rule (TPC-DI spec Clause
   4.5.x) not yet confirmed. Needed for gold's `IsCustomer` column on
   `Prospect`.
