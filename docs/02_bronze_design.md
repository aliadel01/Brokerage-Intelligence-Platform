# Bronze Layer 

## Table of contents
- [Bronze Layer](#bronze-layer)
  - [Table of contents](#table-of-contents)
  - [Governing principle](#governing-principle)
  - [Archetypes](#archetypes)
  - [Source classification table](#source-classification-table)
  - [Open questions deferred to silver (do not resolve in bronze)](#open-questions-deferred-to-silver-do-not-resolve-in-bronze)
  - [Metadata envelope reference](#metadata-envelope-reference)

## Governing principle

**Bronze is an exact 1:1 copy of the source file**. If a source file includes CDC_FLAG/CDC_DSN columns, bronze keeps them exactly as they are — even when the values turn out to be constant or not useful after checking (e.g. always 'I'). Bronze never drops a column the source sent, and never creates or calculates a value the source did not provide.

Any decision about how to understand a column — "treat this as append-only instead of tracking state," "combine these two sets of codes," "carry forward the last known value for a partial update" — belongs in the silver layer. Bronze answers "what did the source send," never "what does it mean."

> [!NOTE]
>  The only exception to this 1:1 principle is the schema-shifting CDC archetype (Archetype B). Batch1 of these sources has no CDC columns at all, but Batch2/3 add them. Bronze must backfill the missing columns for Batch1 rows with `_cdc_flag = 'I'` and `_cdc_dsn = 0` so that all batches can land in a single target table with a consistent schema.
> 
## Archetypes
Our 21 sources are not homogeneous. There are really **five distinct source archetypes** hiding in this data dictionary, and each needs a different strategy:

| Archetype | Sources | Defining trait | Bronze implication |
| --- | --- | --- | --- |
| **A. Static/reference dimensions** | HR, Date, Time, StatusType, TaxRate, Industry, TradeType | Loaded once (Batch1), never change again | Simple full-load, no dedup logic needed, no CDC handling |
| **B. Schema-shifting CDC facts** | Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket | Column count differs between Batch1 and Batch2/3 | Bronze must absorb both shapes into one target schema; CDC columns must be defaulted (`_cdc_flag = 'I'`, `_cdc_dsn = 0`) for Batch1 rows |
| **C. Snapshot/full-refresh dimensions** | Prospect | No CDC, but re-extracted in full every batch | Bronze must not naively append without context — needs a batch-tagged full snapshot pattern |
| **D. Non-tabular / structural outliers** | CustomerMgmt.xml (nested XML), FINWIRE (fixed-width, 3 record types in one file) | Require pre-parsing before they can even become "rows" | Bronze needs a flattening/parsing sub-stage before landing; can't be a pure 1:1 copy of the file |
| **E. Batch1-only fact data** | TradeHistory | Loaded once (Batch1), never change again | Simple full-load, no dedup logic needed, no CDC handling; but unlike Archetype A, this is *fact* data, not static reference data |


## Source classification table

| # | Source file | Bronze table | Archetype | CDC columns in bronze? | Reasoning |
|---|---|---|---|---|---|
| 1 | Date.txt | `bronze_date` | A | No | Static reference, Batch1 only, source has no CDC columns |
| 2 | Time.txt | `bronze_time` | A | No | Same as above |
| 3 | StatusType.txt | `bronze_status_type` | A | No | Same as above |
| 4 | TaxRate.txt | `bronze_tax_rate` | A | No | Same as above |
| 5 | Industry.txt | `bronze_industry` | A | No | Same as above |
| 6 | TradeType.txt | `bronze_trade_type` | A | No | Same as above |
| 7 | HR.csv | `bronze_hr` | A | No | Same as above |
| 8 | Prospect.csv | `bronze_prospect` | C | No | Every batch is a complete re-extract, not a delta; no reliable key uniqueness guaranteed within one batch |
| 9 | Account.txt | `bronze_account` | B | Yes — **real CDC** | `CDC_FLAG` genuinely varies I/U and reflects real attribute updates to an existing account |
| 10 | Customer.txt | `bronze_customer` | B | Yes — **real CDC** | Same as above, for customer attributes |
| 11 | Trade.txt | `bronze_trade` | B | Yes — **real CDC** | Batch1 has no CDC columns at all (different field count); Batch2/3 add them. Backfilled `('I', 0)` for Batch1 |
| 12 | TradeHistory.txt | `bronze_trade_history` | E | No | Loads once in Batch1 only (like A), but is trade-lifecycle *fact* data linked to `bronze_trade` via `T_ID` — not static reference data. No Batch2/3 counterpart exists at all |
| 13 | HoldingHistory.txt | `bronze_holding_history` | B | Yes (quasi-CDC) | File format carries CDC columns; bronze stores them as-is. Whether to treat as append-only is a silver decision |
| 14 | WatchHistory.txt | `bronze_watch_history` | B | Yes (quasi-CDC) | `CDC_FLAG` observed constant `'I'` — "rows only added" per spec — but still stored verbatim in bronze |
| 15 | DailyMarket.txt | `bronze_daily_market` | B | Yes (quasi-CDC) | Same reasoning as WatchHistory |
| 16 | CashTransaction.txt | `bronze_cash_transaction` | B | Yes (quasi-CDC) | Same reasoning as WatchHistory |
| 17 | CustomerMgmt.xml (customer) | `bronze_mgmt_customer` | D | No (uses `ActionType` instead) | Batch1/Historical Load only. `ActionType` carries richer state info (6 values) than Account/Customer.txt's 2-value CDC_FLAG — not a 1:1 vocabulary, unification deferred to silver |
| 18 | CustomerMgmt.xml (account) | `bronze_mgmt_account` | D | No (uses `ActionType` instead) | Same as above |
| 19 | FINWIRE — CMP | `bronze_finwire_cmp` | D | No | Batch1 only, append-only-by-PTS, no CDC columns in file |
| 20 | FINWIRE — SEC | `bronze_finwire_sec` | D | No | Same as above |
| 21 | FINWIRE — FIN | `bronze_finwire_fin` | D | No | Same as above |
| 22 | BatchDate.txt | `bronze_batch_control` | **Operational/Control** | No | Not business data — records pipeline execution metadata (as-of date per batch) |
| 23 | *_audit.csv | `bronze_source_audit` | **Operational/Control** | No | Not business data — vendor-supplied reconciliation counts for QA |


## Open questions deferred to silver (do not resolve in bronze)

1. **`bronze_customer`/`bronze_account` vs `bronze_mgmt_customer`/
   `bronze_mgmt_account` unification.** `Account.txt`/`Customer.txt` carry a
   real `CA_ST_ID`/`C_ST_ID` status value straight from the source. The XML
   tables carry no status column at all — status must be *derived* from
   `ActionType` (`CLOSEACCT`/`INACT` → some inactive/closed status value).
   This is a materially different trust level (source-provided vs.
   derived) and must be documented explicitly wherever the unified silver
   model surfaces a `status` column.
2. **Sparse `UPDCUST`/`UPDACCT` payloads.** Per spec, these actions send
   only `C_ID`/`CA_ID` plus the fields that changed; everything else is
   NULL on that row. Reconstructing "current full record" via
   `COALESCE`/carry-forward logic (e.g. `LAG(...) IGNORE NULLS` or a
   forward-fill window function) happens in silver, not bronze.
3. **Append-only reinterpretation of quasi-CDC tables.** `holding_history`,
   `watch_history`, `daily_market`, `cash_transaction` all carry CDC
   columns in bronze per the 1:1 principle, but are treated as append-only
   event logs (dedup via `_row_hash`, not "latest `_cdc_dsn` wins") in
   silver, because their `CDC_FLAG` doesn't reflect genuine state updates.

## Metadata envelope reference

Every bronze table carries:

| Column | Type | Purpose |
|---|---|---|
| `_batch_id` | `NUMBER(9,0)` | Which load batch produced this row |
| `_source_file` | `VARCHAR` | Exact filename ingested |
| `_loaded_at` | `TIMESTAMP_NTZ(3)` | Ingestion wall-clock time |
| `_row_hash` | `NUMBER(20,0)` | Deterministic hash of business columns — QA/dedup signal |
| `_dq_errors` | `VARIANT` | JSON array/object storing data quality validation rules output; NULL if no errors |

> [!NOTE]
> `bronze_batch_control` does not carry a `_dq_errors` column. Batch idempotency and re-ingestion detection (see Batch idempotency / re-ingestion) depend on this table having a valid asofdate for every batch — a NULL here would break that check, not just degrade data quality. So `load_batch_date` raises instead of soft-failing on a bad cast (see Failure handling): a row only ever lands in this table when asofdate is valid. Since every row is clean by construction, there's nothing for `_dq_errors` to ever record.

CDC-capable sources (Archetype B) additionally carry:

| Column | Type | Purpose |
|---|---|---|
| `_cdc_flag` | `VARCHAR(1)` | Verbatim from source; backfilled to `'I'` only where Batch1 has no CDC columns at all (schema-shift case) |
| `_cdc_dsn` | `NUMBER(20,0)` | Verbatim from source; backfilled to `0` under the same condition |