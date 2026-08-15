# 4. Silver Layer

**Scope:** every interpretive decision `02_bronze_design.md` §2.4
deferred. Silver is where "what did the source send" (bronze) becomes
"what does it mean" (business-usable). This is the layer with the most
judgment calls in the platform — recorded here as ADRs, not just code,
so the *reasoning* survives a reviewer who wasn't in the room when the
decision was made.

## Table of contents
- [4. Silver Layer](#4-silver-layer)
  - [Table of contents](#table-of-contents)
  - [4.1 Governing Principle](#41-governing-principle)
  - [4.2 Strategies by Archetype](#42-strategies-by-archetype)
  - [4.3 Architecture Decision Records](#43-architecture-decision-records)
    - [ADR-001: Account/Customer](#adr-001-accountcustomer)
    - [ADR-002: Trade](#adr-002-trade)
    - [ADR-003: Two dedup shapes, one macro](#adr-003-two-dedup-shapes-one-macro)
    - [ADR-004: Custom `generate_schema_name` macro](#adr-004-custom-generate_schema_name-macro)
    - [ADR-005: Incremental append materialization for quasi-CDC](#adr-005-incremental-append-materialization-for-quasi-cdc)

---

## 4.1 Governing Principle

Silver answers **what does it mean**. Bronze answers **what did the
source send** (`02_bronze_design.md` §2.1). Every interpretive decision
bronze leaves open — unifying two vocabularies, choosing append-only
vs. state-tracking, deriving a value the source never sent — is
resolved here, not upstream and not deferred further downstream.

Silver models clean, cast, dedup, and (where needed) historize data.
They do not touch the raw metadata envelope beyond carrying `_batch_id`
forward for lineage (`07_governance.md` §7.4).

Two dedup shapes are used across this layer, chosen by source type, not
habit — both route through one shared `dedup_latest(relation,
partition_by, order_by)` macro, never an inline `qualify`:

- **State-tracking dedup** (static dimensions, Prospect, Trade): one
  row per business key, latest wins.
- **Append-only dedup** (quasi-CDC event logs; the safety pass inside
  `silver_account`/`silver_customer`; the final duplicate-ingestion
  pass inside `silver_trade_history`): removes exact duplicate rows
  only — every distinct event stays.

---

## 4.2 Strategies by Archetype

| Archetype (per `02_bronze_design.md` §2.2) | Sources | Strategy |
|---|---|---|
| A. Static/reference | Date, Time, StatusType, TaxRate, Industry, TradeType, HR | Pass-through + defensive dedup. No CDC, no history. |
| B. Real CDC | Account, Customer | SCD2, day-level grain. Unifies a flat-file source and an XML source. |
| B. Real CDC (latest-state) | Trade | State-tracking dedup, one row per `trade_id`, latest wins — same shape as Prospect. Status-transition history owned separately by `silver_trade_history` (ADR-002). |
| B. Quasi-CDC | HoldingHistory, WatchHistory, DailyMarket, CashTransaction | Append-only event log. Materialized `incremental`/`append`, filtered on `_loaded_at` (ADR-005). |
| C. Snapshot dimension | Prospect | SCD1, one row per `agency_id`, latest batch wins. |
| D. FINWIRE | CMP, SEC, FIN | Append-only-by-PTS, Batch1 only. |
| D. CustomerMgmt.xml | mgmt_customer, mgmt_account | Not modeled standalone — folded into `silver_account`/`silver_customer`. |
| E. Batch1-only fact (bronze), cross-batch model (silver) | TradeHistory | Bronze source is Batch1-only; `silver_trade_history` unions it with Batch2/3 status transitions derived from `bronze_trade`, covering the full trade lifecycle across every batch (ADR-002). |

---

## 4.3 Architecture Decision Records

### ADR-001: Account/Customer

**Decision:**
- **Sources unified:** 
  - flat-file CDC (`bronze_account`/`bronze_customer`)
  + XML (`bronze_mgmt_account`/`bronze_mgmt_customer`).
- **Grain:** one row per entity per day. Keep the last event inside
  that day, and only if it differs from the previous kept day's version
  on tracked columns. *Reason: only the final Account/Customer version
  per day is needed.*
- **XML `actiontype` → `cdc_flag`:** Account: `NEW→I, ADDACCT→U,
  CLOSEACCT→U, UPDACCT→U`. Customer: `NEW→I, UPDCUST→U, INACT→U`.
  *Reason: these values aren't real `actiontype`s — the true status
  lives in `ca_st_id`/`c_st_id` on the flat-file source, which already
  carries a real `_cdc_flag`.*
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
  stream) and stay excluded. `INACT` carries zero attributes too, but
  per spec it's a real customer state signal ("has become inactive"),
  so it's included.*
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
  individual event first (full chronological order) before the
  day-level collapse to its last row. *Reason: XML actions (e.g.
  `UPDACCT`) can send a partial payload — collapsing before
  forward-filling was tried and caused a real bug, an early event's
  value could miss the later, kept row.*
- **Ordering key:** `_batch_id, action_ts, _cdc_dsn, _loaded_at` for
  both dedup and forward-fill. *Reason: the XML historical batch shares
  one `_batch_id` and a fixed `_cdc_dsn = 0` across several events for
  the same entity — without `action_ts`, those events tie and sort
  undefined. Safe only because a batch never mixes flat-file and XML
  rows (confirmed).*
- **Surrogate key:** `surrogate_key(['account_id', '_batch_id',
  'valid_from_date'])` and `surrogate_key(['customer_id', '_batch_id',
  'valid_from_date'])`. *Reason: the day-level grain requires
  `valid_from_date`.*

### ADR-002: Trade

**Decision:** Trade is split across two models, by concern.
`bronze_trade` (Batch2/3) naturally arrives as one CDC event per status
change — multiple rows per `trade_id`, the same shape Account/Customer's
CDC sources have. Trade deliberately does **not** keep that natural
event shape in a single model, unlike Account/Customer:

- **`silver_trade`:** one row per `trade_id` — latest known state.
  State-tracking dedup, same shape as `silver_prospect`. Deliberately
  collapses `bronze_trade`'s natural multi-row-per-trade CDC shape down
  to one row. No tracked/carried-only distinction and no forward-fill
  apply — every column is simply "latest known value," and each event
  was confirmed to always send a full payload (no gap to fill).
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
  provides. A `_source_model` column (`'bronze_trade_history'` vs.
  `'bronze_trade'`) records which bronze source produced each row,
  rather than relying on `_batch_id = 1` as an implicit proxy.

**Reason:** the gold-layer target is a Kimball star schema. A
`fact_trade` built directly on `bronze_trade`'s natural
event-per-status-change shape would force every downstream aggregate
(`SUM(commission)`, etc.) to filter to a single current version, or
silently multiply those measures by however many status transitions
the trade went through — a classic **Kimball fan trap**. Splitting
"current economic state" from "status journey" into two models removes
that failure mode structurally instead of relying on query discipline.
→ carried forward into `05_gold.md` ADR-009.

### ADR-003: Two dedup shapes, one macro


**Decision:** all dedup goes through `dedup_latest`. Static dimensions,
Prospect, and Trade (`silver_trade`) use state-tracking dedup (one row
per key, latest wins). Quasi-CDC event logs, the SCD2 models
(Account/Customer), and `silver_trade_history` use duplicate-removal
dedup (keep every distinct event/version).

**Reason:** these two source shapes need different logic; one macro
keeps that logic in one place instead of scattered inline `qualify`
clauses. Trade sits in the state-tracking bucket despite its bronze
source having real CDC columns (`_cdc_flag`/`_cdc_dsn`) — see ADR-002:
`silver_trade` is deliberately latest-state only, not versioned, so it
needs the same shape as a snapshot dimension, not the SCD2 shape its
bronze archetype would otherwise suggest.

### ADR-004: Custom `generate_schema_name` macro


**Decision:** override `generate_schema_name` so a model's `+schema:`
config is used exactly as given, instead of dbt's default
`<target_schema>_<custom_schema>` prefix.

**Reason:** standard, well-known dbt pattern — not specific to this
data model.

### ADR-005: Incremental append materialization for quasi-CDC


**Decision:**
- HoldingHistory, WatchHistory, DailyMarket, CashTransaction are
  materialized `incremental` with `incremental_strategy='append'`,
  `on_schema_change='fail'`, and no `unique_key`.
- Incremental filter: `_loaded_at > max(_loaded_at) in this)`, applied
  on the source CTE before dedup.
- Tie-break rationale (`_batch_id desc, _cdc_dsn desc, _loaded_at
  desc`) is not documented inline beyond the ordering itself. *Reason:
  with true insert-only sources, no event legitimately produces more
  than one row — the dedup pass only exists to catch accidental
  duplicate ingestion, and any deterministic pick among true duplicates
  is equally correct. A different batch naturally sorts by recency; a
  genuine duplicate shares the same `_cdc_dsn`, so the field carries no
  decision weight; `_loaded_at` only breaks a true tie.*
- `on_schema_change='fail'` is deliberate, not left at dbt's default.
  *Reason: silver casts/derives columns explicitly; an unexpected
  schema change upstream should stop the run and surface for review,
  not silently ignore or append new columns.*

**Reason:** these sources are true insert-only event logs — no event
is ever revised by a later row (per `01_sources.md`'s quasi-CDC note).
Re-running a full rebuild on every invocation re-scans and re-dedups
the entire history for no benefit; filtering to unseen `_loaded_at`
values and appending only the new, deduped slice gets the same result
at a fraction of the cost. No `unique_key` is needed because there is
no "latest state" to upsert into — every kept row is independent and
permanent.