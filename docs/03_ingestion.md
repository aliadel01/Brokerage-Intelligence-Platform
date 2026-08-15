# 3. Ingestion Layer

**Scope:** the Python loaders that turn the 23 sources defined in
`01_sources.md`, classified into the archetypes of `02_bronze_design.md`
§2, into bronze rows. This is the **mechanical** implementation of
§1/§2's decisions — it does not re-decide archetype or schema, it
executes what was already decided.

## Table of contents
- [3. Ingestion Layer](#3-ingestion-layer)
  - [Table of contents](#table-of-contents)
  - [3.1 Governing Principle \& Architecture](#31-governing-principle--architecture)
  - [3.2 Loader-to-Source Mapping](#32-loader-to-source-mapping)
  - [3.3 Key Design Decisions, Per Loader](#33-key-design-decisions-per-loader)
    - [3.3.1 `delimited_loader.py` — schema-shift detection by field count](#331-delimited_loaderpy--schema-shift-detection-by-field-count)
    - [3.3.2 `finwire_loader.py` — polymorphic fixed-width dispatch](#332-finwire_loaderpy--polymorphic-fixed-width-dispatch)
    - [3.3.3 `xml_loader.py` — streaming flatten of nested XML into two tables](#333-xml_loaderpy--streaming-flatten-of-nested-xml-into-two-tables)
    - [3.3.4 `audit_loader.py` — two small, simple control-file loaders](#334-audit_loaderpy--two-small-simple-control-file-loaders)
    - [3.3.5 `common.py` — shared primitives](#335-commonpy--shared-primitives)
    - [3.3.6 `snowflake_client.py` — the load primitive](#336-snowflake_clientpy--the-load-primitive)
  - [3.4 Batch Idempotency / Re-Ingestion](#34-batch-idempotency--re-ingestion)
  - [3.5 Reconciliation Check](#35-reconciliation-check)
    - [For Operational Logging](#for-operational-logging)
  - [3.6 Failure Handling](#36-failure-handling)
  - [3.7 Chunk-buffered staging pipeline](#37-chunk-buffered-staging-pipeline)
  - [3.8 Data Quality — Pointer](#38-data-quality--pointer)
  - [Open items](#open-items)

---

## 3.1 Governing Principle & Architecture

Every loader exists to answer one question: **turn whatever shape a
source file arrives in into a staging CSV whose column order and types
match the target bronze table exactly**, then hand off to Snowflake's
bulk-load path (`PUT` + `COPY INTO`). No loader applies business logic,
deduplication-by-meaning, or cross-row reasoning — that belongs to
silver (`02_bronze_design.md` §2.1). Ingestion's only responsibilities:
**parse correctly, cast safely, preserve lineage, load efficiently.**

```
main.py (CLI entry point)
│
├─ snowflake_client.py    connection + PUT/COPY INTO primitive
├─ common.py              shared casters, row-hash, streaming CSV writer
├─ config.py               declarative per-source column/type mapping
│                          + ALL_BRONZE_TABLES (used by --force wipe)
│
├─ loaders/delimited_loader.py   generic CSV/PSV loader (15 sources)
├─ loaders/finwire_loader.py     fixed-width, 3-record-type dispatcher
├─ loaders/xml_loader.py         nested XML flattener (CustomerMgmt.xml)
└─ loaders/audit_loader.py       control/operational tables
```

`main.py`'s `run_batch()` executes one batch directory, step by step:

1. Check `bronze_batch_control` for an existing row for this
   `_batch_id`. Found + no `--force` → stop immediately. Found +
   `--force` → delete this batch's rows from every bronze table first
   (§3.4).
2. `BatchDate.txt` → `bronze_batch_control` (loaded first, deliberately
   — see §3.4). **Required, not optional**: missing file stops the
   batch right here instead of quietly proceeding without a control row.
3. Configured delimited sources → `CustomerMgmt.xml` → `FINWIRE*` files
   → `*_audit.csv`.
4. Reconciliation check: compare `bronze_source_audit`'s expected row
   counts against what was actually loaded this run (§3.5).

A source absent from the batch directory is skipped silently, no
error — batch scope (Batch1 vs. Batch2/3, per `01_sources.md` §1.1) is
determined by file presence, not hardcoded. `BatchDate.txt` is the one
exception to this skip-if-missing rule.

---

## 3.2 Loader-to-Source Mapping

| Source | Loader | Notes |
|---|---|---|
| Date, Time, StatusType, TaxRate, Industry, TradeType, HR, Prospect, Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket, CashTransaction, TradeHistory | `delimited_loader.load_delimited_source` | Driven by `config.DELIMITED_SOURCES`; one generic function handles all 15 |
| `CustomerMgmt.xml` | `xml_loader.load_customer_mgmt_xml` | Streaming XML parse, flattens to two tables |
| `FINWIRE*` | `finwire_loader.load_finwire_source` | Fixed-width, 3 interleaved record types dispatched by `RecType` |
| `*_audit.csv` | `audit_loader.load_audit_source` | Vendor reconciliation counts |
| `BatchDate.txt` | `audit_loader.load_batch_date` | Single-value control file; required (§3.4) |

---

## 3.3 Key Design Decisions, Per Loader

### 3.3.1 `delimited_loader.py` — schema-shift detection by field count

Reads any pipe/comma-separated source line by line. For each line, it
determines whether `CDC_FLAG`/`CDC_DSN` columns are present, casts
every business field via `_safe_cast`, computes a row hash, and writes
to a staging CSV — then hands off to `copy_into()`.

`Trade.txt`, `HoldingHistory.txt`, `WatchHistory.txt`,
`DailyMarket.txt` have fewer columns in Batch1 than Batch2/3
(`01_sources.md` §1.1). Instead of hardcoding "Batch1 = no CDC,"
`_split_cdc()` reads each line's actual field count:

- `field_count == base_column_count` → no CDC columns; backfill `('I', 0)`.
- `field_count == base_column_count + 2` → CDC columns present, use them.
- anything else → hard `ValueError` — stop loud, don't silently misalign.

This makes the loader driven by the file's actual shape, not the batch
number — it stays correct even if a schema shift lands on a different
batch than expected.

> [!NOTE] Each delimited source has a `cdc_capable` flag in `config.py` so `_split_cdc()` knows whether to expect CDC columns at all.


### 3.3.2 `finwire_loader.py` — polymorphic fixed-width dispatch

Reads FINWIRE's fixed-width lines one at a time. Every line starts with
a `PTS` timestamp (15 chars) and `RecType` (3 chars: `CMP`/`SEC`/`FIN`).
Based on `RecType`, remaining fields are sliced by fixed width, cast
field by field, and routed to one of three `StreamingCsvWriter`s open
simultaneously — memory use stays low regardless of file size. Each of
the three staging files loads to its own bronze table at the end.

`CoNameOrCIK` (one field, two possible meanings) is resolved at parse
time via `_resolve_co_name_or_cik()`: numbers-only → CIK, anything else
→ company name, per spec.

An unrecognized `RecType` raises `ValueError` immediately — never a
silently-dropped line.

### 3.3.3 `xml_loader.py` — streaming flatten of nested XML into two tables

Streams `CustomerMgmt.xml` one `Action` element at a time
(`ET.iterparse`, whole file never loaded into memory). For each
`Action`, pulls `Customer` fields (name, address, contact, tax info,
phone numbers) into the customer staging file; for every nested
`Account`, writes one more row to the account staging file. Both load
to `bronze_mgmt_customer`/`bronze_mgmt_account` at the end.

Three points confirmed against a real sample file + the published XSD
(not assumed from spec prose):
1. `Name`/`Address`/`ContactInfo`/`TaxInfo` are genuinely nested
   elements under `Customer`, each optional (`minOccurs="0"`).
2. Sparse `UPDCUST` payloads are real — a live sample showed `C_TIER`
   absent on an `UPDCUST` action, confirming `minOccurs="0"` over the
   spec's misleading prose ("Not empty").
3. `C_PHONE_1`/`C_PHONE_2`/`C_PHONE_3` are each a nested `PhoneNumber`
   element (`C_CTRY_CODE`/`C_AREA_CODE`/`C_LOCAL`/`C_EXT`), not flat
   fields.

Every optional field is read via `.get()`/`.findtext()`, both
`None`-returning on absence — a missing value becomes `NULL` on
`COPY INTO` with no extra handling. **No `COALESCE`/carry-forward logic
runs here** — reconstructing "current full record" from a sparse update
stream is a silver decision (`02_bronze_design.md` §2.4, item 2).

Column naming for the phone fields (`c_ctry_1`, `c_area_1`, `c_local_1`,
`c_ext_1`, ...) deliberately matches `Customer.txt`'s flattened naming,
ahead of the silver-layer unification.

Uses `ET.iterparse(..., events=("end",))` with `action.clear()` after
each `Action` — memory footprint stays close to one `Action` block, not
the whole document.

### 3.3.4 `audit_loader.py` — two small, simple control-file loaders

`load_audit_source()` reads a vendor CSV (with header-whitespace
stripping — vendors occasionally send `"DataSet "` with a trailing
space), casts each field, loads into `bronze_source_audit`.

`load_batch_date()` reads `BatchDate.txt`'s single line, casts it as a
date, inserts one row into `bronze_batch_control`. This is the one
loader in the codebase that **raises on a bad cast** instead of loading
`NULL` and continuing — this date drives the entire batch-idempotency
check (§3.4), so a `NULL` here would quietly break that safety net for
every future run. Full DQ reasoning: `06_data_quality.md` §6.1.4.

### 3.3.5 `common.py` — shared primitives

- **`_safe_cast(raw, caster, col_name)`** — strips whitespace, returns
  `None` on empty string or any exception from `caster`, plus structured
  error details so nothing is lost silently (full DQ design:
  `06_data_quality.md` §6.1.2–6.1.3). **Caveat:** only works if `caster`
  can actually raise (e.g. `int`, `parse_date`) — a plain-`str` caster
  never raises, so a corrupt value in a `str`-cast column passes through
  unchanged, with Snowflake's own type check as the only backstop.
- **`compute_row_hash(values)`** — BLAKE2b digest (8 bytes) over
  `"|"`-joined business column values; QA/dedup/lineage signal across
  every loader.
- **`StreamingCsvWriter` / `write_staging_csv`** — bounded-memory CSV
  writers (5,000-row chunks): the former for loaders fanning one input
  stream to multiple target files at once (FINWIRE, XML), the latter for
  single-target loaders.
- **`format_csv_value`** — converts Python values into the exact string
  form Snowflake's `ff_bronze_csv` file format expects (empty string
  for `NULL`, `TRUE`/`FALSE` for booleans, millisecond-truncated
  timestamps).
- **`_pack_dq_errors`** — takes a list of `_safe_cast`-produced error dicts and
  returns a single JSON string for the `_dq_errors` column, or `None` if
  no errors. This is the only place in the codebase that touches
  `_dq_errors` directly; every loader calls it after casting all fields
  to produce the final value for that column.

### 3.3.6 `snowflake_client.py` — the load primitive

One function, `copy_into()`, used by every loader at the end of its
work. `PUT`s the staging CSV to Snowflake's internal stage, then runs
`COPY INTO` with an **explicit column list** (never positional-only) —
a loader's output and the target table's shape can't silently drift
apart, even if one side changes independently. `ON_ERROR =
'ABORT_STATEMENT'` means a single row with a genuinely bad value stops
the whole file's load — which is exactly why upstream `_safe_cast`
matters: it's the main thing standing between a dirty row and a stopped
batch.

---

## 3.4 Batch Idempotency / Re-Ingestion

Every bronze table (business data + the two control tables) carries a
`_batch_id` column. `run_batch()` checks `bronze_batch_control` for
that `_batch_id` before doing anything else — but only **after**
confirming `BatchDate.txt` exists:

- **No existing row:** proceeds normally.
- **Existing row, `--force` not passed:** raises `RuntimeError`
  immediately, nothing loaded — running the same `--batch-id` twice by
  accident must be loud, not silent.
- **Existing row, `--force` passed:** `force_delete_batch()` deletes
  every row for that `_batch_id` from every table in
  `config.ALL_BRONZE_TABLES` (business + both control tables), inside
  one transaction — a mid-loop failure rolls the whole wipe back
  instead of leaving bronze half-cleaned. `run_batch()` then proceeds as
  a fresh load.

`bronze_batch_control` loads **first among actual sources** — so an
interrupted run is correctly seen as "already attempted" on the next
run, instead of silently reloading everything.

**`BatchDate.txt`'s existence is checked before the idempotency query
and before `--force` can touch anything.** This whole safety mechanism
depends on that file being loadable, so its presence is confirmed
first: `run_batch()` raises `FileNotFoundError` before querying
`bronze_batch_control` at all. Unlike every other source (silently
skipped if missing, since batch scope is file-presence-driven),
`BatchDate.txt` is required — the old behavior skipped it silently,
meaning every *other* source in the batch would still load fine, but
the batch would finish without ever writing a row to
`bronze_batch_control`. The next run for that `batch_id` would then see
`exists = False`, assume the batch was never attempted, and load
everything again on top — no warning, no dedup. Checking file
existence before the idempotency check closes that gap at the source.

> [!IMPORTANT] Two things this design deliberately does **not** do:
> - Distinguish "batch completed successfully" from "batch partially
  loaded, then failed" — both look identical (existing row found →
  block unless `--force`). Recovery is always full re-run with
  `--force` (wipe + reload); there is no "resume from point of failure"
  path (see [Open items](#open-items)).
> - Scope `--force` to only the control tables — it deletes **all**
  bronze data for the batch, including business tables. Deliberate:
  business tables are already protected downstream in silver by
  `dedup_latest`/`_row_hash` (`04_silver.md`), so a stray duplicate
  load was never the real risk. The goal here is avoiding storage bloat
  from repeated partial loads sitting in bronze indefinitely.

---

## 3.5 Reconciliation Check

At the end of `run_batch()`, after every source has loaded, a
`loaded_counts` dict (built during ingestion, keyed by source filename
stem) is compared against `bronze_source_audit`'s vendor-supplied
`RowCount` rows for this `_batch_id` — an independent external source,
per the DQ pattern in `06_data_quality.md` §6.1.8.

The comparison combines two expected-count shapes:
- Sources whose audit file has a single `*_RECORDS`-suffixed attribute
  (e.g. `WH_RECORDS` for `WatchHistory`) — used directly.
- Sources with no such attribute (FINWIRE, Account, Customer,
  CustomerMgmt) — their positive-valued attributes are summed per
  `_source_file` instead (e.g. FINWIRE's `FW_CMP` + `FW_SEC` +
  `FW_FIN`), ignoring negative `_DUP` counters so they don't offset the
  real total.

Additional notes:
- FINWIRE's three target tables are summed into one count under the
  FINWIRE file's stem — the vendor audit file gives one `RowCount` for
  the whole file, not per target table.
- `CustomerMgmt.xml`'s two target tables are summed the same way.
- Matching an audit row's `_source_file` (`"<filename>_audit.csv"`) to
  a `loaded_counts` key is done by removing the `_audit.csv` suffix.

**Every comparison outcome is recorded in `governance.dq_audit_log` via
`log_dq_event()`, not just mismatches:**

| Outcome | `check_type` | `severity` |
|---|---|---|
| counts match | `reconciliation_check` | `PASS` |
| counts differ | `reconciliation_mismatch` | `WARNING` |
| audit expects a source, none loaded | `reconciliation_mismatch` | `WARNING` |

A mismatch is also mirrored to the operational log (§3.6) but never
stops the batch — a mismatch can have a legitimate explanation, so this
is a "someone should notice" signal, not an automatic failure. Logging
passes too, not only failures, means `dq_audit_log` shows full
reconciliation *coverage* per batch, not just exceptions. This check
already caught a real off-by-one in a locally-generated
`WatchHistory`/`Account` sample during testing — confirming the
comparison logic itself works.

### For Operational Logging
The ingestion pipeline decouples **operational/progress logging** from **business/data quality auditing**:

*   **Business DQ Evidence:** Recorded permanently in Snowflake (`governance.dq_audit_log`) via `log_dq_event()` for formal compliance and reconciliation tracking.
*   **Operational Logs:** Standard output (`stdout`) tracking real-time execution progress, file processing events, and runtime warnings/errors.

Passing `--log-file <path>` (e.g., `--log-file ingest.log`) mirrors all operational console logs to a designated local file. 

```bash
# Example usage with operational log file output
python -m ingestion.main --batch-id 1 --log-file ingest.log
```

**Output Example:**
```bash
2026-08-09 19:03:36 [batch 1] INFO BatchDate.txt -> bronze_batch_control
2026-08-09 19:03:40 [batch 1] INFO date: 25933 rows -> bronze_date
2026-08-09 19:03:46 [batch 1] INFO time: 86400 rows -> bronze_time
...
2026-08-09 19:03:48 [batch 1] INFO tax_rate: 320 rows -> bronze_tax_rate
2026-08-09 19:03:49 [batch 1] INFO industry: 102 rows -> bronze_industry
```
## 3.6 Failure Handling

| Failure | Behavior | Why |
|---|---|---|
| Delimited source, unexpected field count | Raises immediately (`_split_cdc`'s guard) | Wrong shape means the code misread the file's structure — continuing would misalign every subsequent column |
| FINWIRE, unrecognized `RecType` | Raises immediately | Same stop-loud principle |
| Malformed individual value (bad date, non-numeric int, etc.) | `NULL` via `_safe_cast`, batch continues | Wrong content in the right shape is normal dirty data — full DQ handling in `06_data_quality.md` §6.1.2–6.1.4 |
| XML `Action` with no `Customer` element | Skipped (`action.clear()` + `continue`) | Defensive guard against a broken document, not expected in normal operation |
| Missing `BatchDate.txt` | Raises `FileNotFoundError` at the very top of `run_batch()` | Load-bearing for the idempotency check itself — see §3.4 for why a silent skip here is unsafe in a way skipping `Prospect.csv` is not |
| Already-ingested batch, no `--force` | Raises `RuntimeError` before any source is touched | Loud on purpose |
| Reconciliation mismatch | Never fatal — `WARNING` row in `dq_audit_log` + mirrored to operational log | §3.5 |

---

## 3.7 Chunk-buffered staging pipeline

The ingestion layer must process multi-gigabyte raw datasets (XML, CSV, fixed-width text) into Snowflake staging schemas under strict memory constraints (e.g., containerized execution environments).

Technical Trade-offs & Analysis

1. In-Memory Full File Loading (`In-Memory DataFrames`)
   * **Trade-off:** High memory consumption; memory scales linearly $O(N)$ with payload size.
   * **Verdict:** **Rejected.** Induces Out-Of-Memory (OOM) crashes on large files (`FINWIRE`, `CustomerMgmt.xml`).

2. Row-by-Row Ingestion (`Line-by-Line DB Writes`)
   * **Trade-off:** Minimal memory footprint, but severe I/O bottlenecks.
   * **Verdict:** **Rejected.** High system call overhead and network latency. Direct individual `INSERT` statements violate Snowflake's columnar bulk-loading requirements.

**Accepted** Architecture Decision: Streaming + Chunked Local Staging
- Implement an **iterative streaming and chunk-buffered staging pipeline** (`common.py`).

Core Design Rules
  * **Iterative Parsing:** Use generators/iterative parsers (`csv.reader`, `xml.etree.ElementTree.iterparse`) to stream source data without loading full files into memory.
  * **Bounded Memory ($O(1)$):** Buffer records in-memory up to a configurable threshold (e.g., 5,000 records). Memory utilization remains constant regardless of file size.
  * **Optimized I/O Flushes:** Perform batch disk writes using optimized array flushes (`writerows()`) to maximize local I/O throughput.
  * **Bulk Loading:** Push generated staging CSVs via Snowflake internal stages using optimized `PUT` and `COPY INTO` commands.
  Source File Stream ──► Memory Buffer (Fixed Chunk Size) ──► Disk Flush (Local CSV) ──► Snowflake PUT / COPY INTO
## 3.8 Data Quality — Pointer

This document covers *how* each loader works — file shape, parsing,
schema detection, load mechanics. The complete data-quality design
(how bad values are caught, recorded, and kept traceable via
`_dq_errors`; the one exception for `BatchDate.txt`'s `asofdate`) lives
in `06_data_quality.md` §6.1 — kept there, not duplicated here, so
there's a single source of truth for DQ reasoning.

Batch-level DQ evidence (reconciliation results, pass and fail) is
recorded separately in `governance.dq_audit_log` — §3.5 above and
`06_data_quality.md` §6.3. `_dq_errors` and `dq_audit_log` operate at
different grains: `_dq_errors` is per row/column, carried on the row
itself; `dq_audit_log` is per batch/per check, in its own table.
Neither is process/debug output — that's the operational log
(`logging_setup.py`), not persisted to Snowflake
(`07_governance.md` §7.9).

---

## Open items

- **No "resume from partial failure" path.** Recovering from an
  interrupted run means re-running the whole batch with `--force`
  (full wipe + reload), not resuming from the point of failure (§3.4).