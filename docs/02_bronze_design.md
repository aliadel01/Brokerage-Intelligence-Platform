# 2. Bronze Layer Design

**Scope:** the landing-zone modeling decisions for all 23 sources
defined in `01_sources.md`. Bronze is where **archetype classification**
happens — the single decision that every later layer (`03_ingestion.md`
through `07_governance.md`) inherits, so it's worth getting right once
here rather than re-litigating per model.\
**Code location:** `sql/bronze_schema.sql`. 

## Table of contents

- [2. Bronze Layer Design](#2-bronze-layer-design)
  - [Table of contents](#table-of-contents)
  - [2.1 Governing Principle](#21-governing-principle)
  - [2.2 Source Archetypes](#22-source-archetypes)
  - [2.3 Source Classification Table](#23-source-classification-table)
  - [2.4 Open Questions Deferred to Silver](#24-open-questions-deferred-to-silver)
  - [2.5 Metadata Envelope](#25-metadata-envelope)
---

## 2.1 Governing Principle

**Bronze is an exact 1:1 copy of the source file.** If a source
includes `CDC_FLAG`/`CDC_DSN` columns, bronze keeps them exactly as
sent — even when the values turn out constant or unhelpful after
inspection (e.g. always `'I'`, see `01_sources.md`'s quasi-CDC note).
Bronze never drops a column the source sent, and never derives or
calculates a value the source didn't provide.

Any interpretive decision — "treat this as append-only instead of
tracking state," "combine these two vocabularies," "carry forward the
last known value for a partial update" — belongs in silver
(`04_silver.md`). Bronze answers **what did the source send**, never
**what does it mean**. This is the same layer-boundary discipline
`04_silver.md` and `05_gold.md` each restate at the top of their own
governing principle — the three layers form one consistent contract,
not three independently-invented ones.

> [!NOTE]
> The one exception to the 1:1 principle is the schema-shifting CDC
> archetype (Archetype B). Batch1 of these sources has no CDC columns
> at all; Batch2/3 add them (`01_sources.md` §1.1). Bronze backfills
> the missing columns for Batch1 rows with `_cdc_flag = 'I'` and
> `_cdc_dsn = 0`, so every batch can land in one target table with a
> consistent schema. This is a **structural** backfill (fixed-shape
> defaults for absent columns), not an interpretive one — it doesn't
> violate the governing principle above.

---

## 2.2 Source Archetypes

**Theory:** a data dictionary of 21 sources is rarely homogeneous.
Naively treating "one loader pattern fits all" is where bronze designs
usually go wrong. This platform identifies **five distinct archetypes**
hiding in the dictionary, each requiring a different ingestion and
schema strategy:

| Archetype | Sources | Defining trait | Bronze implication |
|---|---|---|---|
| **A. Static/reference dimensions** | HR, Date, Time, StatusType, TaxRate, Industry, TradeType | Loaded once (Batch1), never change again | Simple full-load, no dedup logic, no CDC handling |
| **B. Schema-shifting CDC facts** | Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket | Column count differs Batch1 vs. Batch2/3 | Bronze absorbs both shapes into one target schema; CDC columns backfilled (`_cdc_flag='I'`, `_cdc_dsn=0`) for Batch1 rows |
| **C. Snapshot/full-refresh dimensions** | Prospect | No CDC, but re-extracted in full every batch | Bronze must not naively append without context — needs a batch-tagged full-snapshot pattern |
| **D. Non-tabular / structural outliers** | `CustomerMgmt.xml` (nested XML), FINWIRE (fixed-width, 3 record types) | Require pre-parsing before they're even "rows" | Bronze needs a flattening/parsing sub-stage; not a pure 1:1 file copy |
| **E. Batch1-only fact data** | TradeHistory | Loaded once (Batch1), never changes | Simple full-load like Archetype A, but this is *fact* data, not reference data |

Full column-level definitions for every one of these sources:
`01_sources.md` §1.2.

---

## 2.3 Source Classification Table

| # | Source file | Bronze table | Archetype | CDC columns in bronze? | Reasoning |
|---|---|---|---|---|---|
| 1 | Date.txt | `bronze_date` | A | No | Static reference, Batch1 only, no CDC columns in source |
| 2 | Time.txt | `bronze_time` | A | No | Same as above |
| 3 | StatusType.txt | `bronze_status_type` | A | No | Same as above |
| 4 | TaxRate.txt | `bronze_tax_rate` | A | No | Same as above |
| 5 | Industry.txt | `bronze_industry` | A | No | Same as above |
| 6 | TradeType.txt | `bronze_trade_type` | A | No | Same as above |
| 7 | HR.csv | `bronze_hr` | A | No | Same as above |
| 8 | Prospect.csv | `bronze_prospect` | C | No | Every batch is a complete re-extract, not a delta — no reliable key uniqueness within one batch |
| 9 | Account.txt | `bronze_account` | B | Yes — **real CDC** | `CDC_FLAG` genuinely varies I/U, reflects real attribute updates |
| 10 | Customer.txt | `bronze_customer` | B | Yes — **real CDC** | Same, for customer attributes |
| 11 | Trade.txt | `bronze_trade` | B | Yes — **real CDC** | Batch1 has no CDC columns (different field count); Batch2/3 add them, backfilled `('I', 0)` for Batch1 |
| 12 | TradeHistory.txt | `bronze_trade_history` | E | No | Loads once (Batch1), like A, but is trade-lifecycle *fact* data linked to `bronze_trade` via `T_ID` — no Batch2/3 counterpart exists at all |
| 13 | HoldingHistory.txt | `bronze_holding_history` | B | Yes (quasi-CDC) | File carries CDC columns; bronze stores as-is. Append-only reinterpretation is a silver decision |
| 14 | WatchHistory.txt | `bronze_watch_history` | B | Yes (quasi-CDC) | `CDC_FLAG` observed constant `'I'` — still stored verbatim in bronze |
| 15 | DailyMarket.txt | `bronze_daily_market` | B | Yes (quasi-CDC) | Same reasoning as WatchHistory |
| 16 | CashTransaction.txt | `bronze_cash_transaction` | B | Yes (quasi-CDC) | Same reasoning as WatchHistory |
| 17 | CustomerMgmt.xml (customer) | `bronze_mgmt_customer` | D | No — uses `ActionType` instead | Batch1/Historical Load only. `ActionType` carries richer state (6 values) than Account/Customer.txt's 2-value CDC_FLAG — not a 1:1 vocabulary, unification deferred to silver |
| 18 | CustomerMgmt.xml (account) | `bronze_mgmt_account` | D | No — uses `ActionType` instead | Same as above |
| 19 | FINWIRE — CMP | `bronze_finwire_cmp` | D | No | Batch1 only, append-only-by-PTS, no CDC columns in file |
| 20 | FINWIRE — SEC | `bronze_finwire_sec` | D | No | Same as above |
| 21 | FINWIRE — FIN | `bronze_finwire_fin` | D | No | Same as above |
| 22 | BatchDate.txt | `bronze_batch_control` | Operational/Control | No | Not business data — pipeline execution metadata (as-of date per batch) |
| 23 | `*_audit.csv` | `bronze_source_audit` | Operational/Control | No | Not business data — vendor reconciliation counts for QA |

---

## 2.4 Open Questions Deferred to Silver

Deliberately **not** resolved at this layer — each is an interpretive
decision that belongs in `04_silver.md`, listed here so the boundary is
explicit rather than implicit:

1. **`bronze_customer`/`bronze_account` vs. `bronze_mgmt_customer`/
   `bronze_mgmt_account` unification.** `Account.txt`/`Customer.txt`
   carry a real `CA_ST_ID`/`C_ST_ID` status value straight from the
   source. The XML tables carry no status column — status must be
   *derived* from `ActionType` (`CLOSEACCT`/`INACT` → an inactive/closed
   status value). This is a materially different trust level
   (source-provided vs. derived) and must be documented explicitly
   wherever the unified silver model surfaces a `status` column.
   → Resolved in `04_silver.md` ADR-001.
2. **Sparse `UPDCUST`/`UPDACCT` payloads.** Per spec, these actions
   send only `C_ID`/`CA_ID` plus changed fields; everything else is
   `NULL` on that row. Reconstructing "current full record" via
   `COALESCE`/carry-forward logic (`LAG(...) IGNORE NULLS` or a
   forward-fill window function) is a silver concern.
   → Resolved in `04_silver.md` ADR-001, "Forward-fill."
3. **Append-only reinterpretation of quasi-CDC tables.**
   `holding_history`, `watch_history`, `daily_market`,
   `cash_transaction` all carry CDC columns in bronze per the 1:1
   principle, but are treated as append-only event logs (dedup via
   `_row_hash`, not "latest `_cdc_dsn` wins") in silver, because their
   `CDC_FLAG` doesn't reflect genuine state updates.
   → Resolved in `04_silver.md` ADR-005.

---

## 2.5 Metadata Envelope

Every bronze table carries:

| Column | Type | Purpose |
|---|---|---|
| `_batch_id` | `NUMBER(9,0)` | Which load batch produced this row |
| `_source_file` | `VARCHAR` | Exact filename ingested |
| `_loaded_at` | `TIMESTAMP_NTZ(3)` | Ingestion wall-clock time |
| `_row_hash` | `NUMBER(20,0)` | Deterministic hash of business columns — QA/dedup signal |
| `_dq_errors` | `VARIANT` | JSON array/object of cast validation failures; `NULL` if none |

This envelope is the row-level backbone for **lineage**
(`07_governance.md` §7.4) and **data quality traceability**
(`06_data_quality.md` §6.1.11).

> [!NOTE]
> `bronze_batch_control` does not carry a `_dq_errors` column. Batch
> idempotency and re-ingestion detection (`03_ingestion.md` §3,
> "Batch idempotency / re-ingestion") depend on this table having a
> valid `asofdate` for every batch — a `NULL` here would break that
> check, not just degrade data quality. So `load_batch_date` **raises**
> instead of soft-failing on a bad cast (`06_data_quality.md` §6.1.4):
> a row only ever lands in this table when `asofdate` is valid. Since
> every row is clean by construction, there's nothing for `_dq_errors`
> to ever record.

**CDC-capable sources (Archetype B) additionally carry:**

| Column | Type | Purpose |
|---|---|---|
| `_cdc_flag` | `VARCHAR(1)` | Verbatim from source; backfilled to `'I'` only where Batch1 has no CDC columns at all (schema-shift case) |
| `_cdc_dsn` | `NUMBER(20,0)` | Verbatim from source; backfilled to `0` under the same condition |