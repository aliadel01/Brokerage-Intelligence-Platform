## Ingestion Layer

### Table of Contents
1. [Overview](#overview)
2. [Landing Strategy per File Format](#landing-strategy-per-file-format)
3. [Metadata Columns](#metadata-columns)
4. [Table Design Strategy by Archetype](#table-design-strategy-by-archetype)
    - [Archetype A — Static Reference Dimensions](#archetype-a--static-reference-dimensions-date-time-statustype-taxrate-industry-tradetype)
    - [Archetype B — Schema-Shifting CDC Facts](#archetype-b--schema-shifting-cdc-facts-account-customer-trade-holdinghistory-watchhistory-dailymarket)
    - [Archetype C — Full Re-Extract Snapshot](#archetype-c--full-re-extract-snapshot-prospect)
    - [Archetype D — Parsed Structural Sources](#archetype-d--parsed-structural-sources-customermgmtxml-finwire)
5. [Python Ingestion Scripts](#python-ingestion-scripts)
    - [Python ingestion script structure](#python-ingestion-script-structure)
    - [What each file does](#what-each-file-does)
6. [Architecture Design Records (ADRs)](#architecture-design-records-adrs)
7. [Architecture Design Records — Bronze Layer DDL](#architecture-design-records--bronze-layer-ddl)

### Overview
Using Python scripts to implement the ingestion layer.

Our 21 sources are not homogeneous, so a single bronze pattern won't fit all of them. There are really **four distinct source archetypes** hiding in this data dictionary, and each needs a different bronze strategy:

| Archetype | Sources | Defining trait | Bronze implication |
| --- | --- | --- | --- |
| **A. Static/reference dimensions** | Date, Time, StatusType, TaxRate, Industry, TradeType | Loaded once (Batch1), never change again | Simple full-load, no dedup logic needed, no CDC handling |
| **B. Schema-shifting CDC facts** | Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket | Column count differs between Batch1 and Batch2/3 | Bronze must absorb both shapes into one target schema; CDC columns must be defaulted (`_cdc_flag = 'I'`, `_cdc_dsn = 0`) for Batch1 rows |
| **C. Snapshot/full-refresh dimensions** | Prospect | No CDC, but re-extracted in full every batch | Bronze must not naively append without context — needs a batch-tagged full snapshot pattern |
| **D. Non-tabular / structural outliers** | CustomerMgmt.xml (nested XML), FINWIRE (fixed-width, 3 record types in one file), TradeHistory (Batch1-only, no incremental counterpart) | Require pre-parsing before they can even become "rows" | Bronze needs a flattening/parsing sub-stage before landing; can't be a pure 1:1 copy of the file |

Get this classification right first. Almost every downstream design decision (dbt source config, incremental strategy) is a function of which archetype a source belongs to — not of the source individually.

---

### Landing Strategy per File Format

| Format | Sources | Landing approach |
| --- | --- | --- |
| **Pipe-delimited flat file** | Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket, CashTransaction, Date, Time, StatusType, TaxRate, Industry, TradeType, TradeHistory | Parse in Python, stage as normalized CSV, then execute bulk load via `COPY INTO` with `PUT` into an internal stage. One bronze table per file, column-for-column, plus metadata columns. |
| **Comma-delimited** | HR, Prospect | Same bulk `COPY INTO` strategy as above, using comma-delimited staging configuration. |
| **Fixed-width, multi-record-type** | FINWIRE | **Cannot land as one flat table.** Needs a pre-parse step that reads the 15-char PTS + 3-char RecType prefix and routes each line to one of three raw shapes (CMP/SEC/FIN) before insert. Do this parsing in Python — fixed-width substring parsing in SQL is fragile and unreadable. Land as three separate tables: `bronze_finwire_cmp`, `bronze_finwire_sec`, `bronze_finwire_fin`. |
| **Nested XML** | CustomerMgmt.xml | Needs flattening before it's tabular. Two children (Customer attributes, nested Account elements) means this is naturally two output tables: a customer-management-event table and a customer-account-link table, joined by `C_ID`/`ActionTS`. Do this flattening in Python using standard library (`xml.etree.ElementTree`). |
| **Control files (BatchDate.txt)** | BatchDate | Not a bronze data table at all — treat as ingestion metadata/config, read once per run to parameterize the batch ID and as-of date used to tag every other row landed in that run. |
| **Audit CSVs (`*_audit.csv`)** | Per component | Land into a **dedicated reconciliation table** (`bronze_source_audit`), separate from entity bronze tables. This becomes your row-count ground truth for QA. |

---

### Metadata Columns — Apply Uniformly, No Exceptions

Every bronze table, regardless of archetype, carries the same audit envelope:

* `_batch_id` — which batch (1, 2, 3, …) this row was loaded in. Useful for tracking and debugging.
* `_source_file` — literal filename, useful when a source spans multiple physical files (e.g., FINWIRE is quarterly: `FINWIRE2015Q4`, etc.).
* `_loaded_at` — ingestion timestamp (wall clock of the load, not business time).
* `_row_hash` — a hash of the business columns (SHA-256), used later for change detection and dedup logic without relying on CDC_FLAG alone (useful safety net for sources like CashTransaction where CDC presence is unconfirmed in your spec excerpt).

For CDC sources specifically, also standardize:

* `_cdc_flag` (normalized from `CDC_FLAG`, backfilled as `'I'` for Batch1).
* `_cdc_dsn` (normalized from `CDC_DSN`, backfilled as `0` for Batch1).

Keep these prefixed and consistent (`_batch_id`, not `batch_id`) so they're visually distinguishable from business columns in every model and never collide with a real column name.

---

### Table Design Strategy by Archetype

#### Archetype A — Static Reference Dimensions (Date, Time, StatusType, TaxRate, Industry, TradeType)

* **Ingestion behavior:** Plain append-only, loaded once during Batch1.
* **Optimization:** Table sizes are tiny (hundreds to low thousands of rows). No clustering or complex logic required.

#### Archetype B — Schema-Shifting CDC Facts (Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket)

* **Ingestion behavior:** Plain append-only. All versions of rows across batches land as full history in bronze.
* **Batch isolation:** If a batch load fails, re-running is handled cleanly via standard SQL: `DELETE FROM bronze_table WHERE _batch_id = N;` followed by re-ingestion.
* **CDC handling:**
  * `_cdc_flag` and `_cdc_dsn` columns exist across all rows.
  * Batch1 backfills synthetic values (`_cdc_flag = 'I'`, `_cdc_dsn = 0`).
  * WatchHistory and DailyMarket are insert-only (`_cdc_flag = 'I'`).


* **Silver layer resolution:** Downstream queries/models resolve the "current state" using window functions:
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY _cdc_dsn DESC) = 1

```



#### Archetype C — Full Re-Extract Snapshot (Prospect)

* **Design:** Each batch's full extract lands as its own generation, tagged by `_batch_id`. Bronze keeps every batch's full snapshot (don't overwrite) — storage is cheap, and point-in-time comparison of prospect lists across batches becomes trivial.
* **Silver layer resolution:** To query the latest state, downstream models simply filter by the latest batch:
```sql
WHERE _batch_id = (SELECT MAX(_batch_id) FROM bronze_prospect)

```



#### Archetype D — Parsed Structural Sources (CustomerMgmt.xml, FINWIRE)

* **Design:** Land as multiple bronze tables per the parsing done in Python (`bronze_finwire_cmp`, `_sec`, `_fin`; customer-event and customer-account-link tables for XML).
* **Ingestion behavior:** Plain append-only.
* **Polymorphic column resolution:** FINWIRE's `CoNameOrCIK` field is resolved at parse time into two explicit columns (`co_name`, `co_cik`, one of which is `NULL` per row) rather than carrying an ambiguous single column into bronze.

#### TradeHistory (Batch1-Only Fact)

* **Design:** Plain append-only table. It contains no incremental updates or CDC machinery. Loaded once during Batch1.

#### Audit Files (`*_audit.csv`)

* **Design:** Land into one unified `bronze_source_audit` table. This is your reconciliation source of truth — treat it as a first-class bronze table used by automated QA scripts to match ingested row counts against vendor totals.


Use this section at the end of the file:


### Python Ingestion Scripts

The ingestion layer is implemented as a small Python package under the `ingestion/` folder. The structure below separates configuration, shared utilities, Snowflake access, and format-specific loaders so each source can be handled independently.

#### Python ingestion script structure

```text
ingestion/
├── .env
├── common.py
├── config.py
├── main.py
├── requirements.txt
├── snowflake_client.py
├── ddl/
│   └── bronze_schema.sql
└── loaders/
    ├── audit_loader.py
    ├── delimited_loader.py
    ├── finwire_loader.py
    └── xml_loader.py
```

#### What each file does
 
| File | Responsibility |
| --- | --- |
| `.env` | Local-only Snowflake credentials and `DATA_DIR`, read by `main.py` via `python-dotenv`. Never committed. |
| `common.py` | Shared primitives used by every loader: the caster lambdas (`parse_date`, `parse_datetime`, `parse_bool`, `parse_yyyymmdd`), `_safe_cast` (null-safe type casting), `format_csv_value` (renders Python values into the exact string form `ff_bronze_csv` expects), `compute_row_hash` (deterministic 64-bit business-column hash), and the two staging writers — `write_staging_csv` for single-table sources and `StreamingCsvWriter` for sources that fan one input pass out to multiple target tables. Both writers are chunked; see ADR-009. |
| `config.py` | Declarative registry of every delimited source: filename, delimiter, target table, CDC capability, and ordered `(column_name, caster)` pairs. `delimited_loader.py` is generic and just reads this config — adding a new pipe/comma source means adding an entry here, not writing a new loader. |
| `main.py` | CLI entry point. Resolves connection/config args (CLI flags or `.env`), opens the Snowflake connection, and runs `run_batch()` for a given batch directory: BatchDate control file first, then all configured delimited sources present in the directory, then `CustomerMgmt.xml` if present, then any `FINWIRE*` files, then any `*_audit.csv` files. Sources absent from a batch directory are skipped — scope is driven by file presence, not a hardcoded batch matrix. |
| `requirements.txt` | Runtime dependencies: `snowflake-connector-python`, `python-dotenv`, `pandas`. |
| `snowflake_client.py` | `get_connection()` opens the Snowflake session. `copy_into()` implements the stage-and-load primitive every loader uses: `PUT` the local staging CSV to the internal stage, then `COPY INTO` the target table with an explicit column list, then return rows loaded from the `COPY INTO` result. |
| `ddl/bronze_schema.sql` | DDL for every bronze table plus the `ingest_stage` internal stage and `ff_bronze_csv` file format (`NULL_IF=('')`, `EMPTY_FIELD_AS_NULL=TRUE`) that the staging CSVs are written to match. |
| `loaders/audit_loader.py` | Two loaders: `load_audit_source()` normalizes vendor `*_audit.csv` reconciliation files into `bronze_source_audit`; `load_batch_date()` reads the single as-of date out of `BatchDate.txt` into `bronze_batch_control`. |
| `loaders/delimited_loader.py` | Generic loader for every pipe/comma source described in `config.py`. Per ADR-001, it detects CDC-column presence per line (by field count) rather than assuming it from the batch number, so Batch1's historical shape and Batch2/3's incremental shape both resolve to the same target schema. |
| `loaders/finwire_loader.py` | Parses the fixed-width, multi-record-type FINWIRE files: 15-char PTS + 3-char RecType prefix routes each line to CMP/SEC/FIN field specs, resolves the trailing `CoNameOrCIK` field into explicit `CoName`/`CoCIK` columns, and lands the three record types into three separate bronze tables in a single pass over the file (ADR-010). |
| `loaders/xml_loader.py` | Flattens `CustomerMgmt.xml` (ADR-002) into `bronze_customer_mgmt_event` (one row per Action/Customer) and `bronze_customer_mgmt_account` (one row per nested Account, linked by `C_ID`). Parses via `iterparse` with element clearing rather than a full-DOM parse (ADR-011), writing both tables concurrently in one pass (ADR-010). |


### Architecture Design Records (ADRs)
 
* **ADR-01 — CDC presence detected per line, not per batch.** Sources like Trade, HoldingHistory, WatchHistory, and DailyMarket carry `CDC_FLAG`/`CDC_DSN` in Batch2/3 but not in Batch1. Rather than hardcoding "Batch1 = no CDC" per source, `delimited_loader.py` compares each line's field count against the configured column count and classifies it as backfilled (`base_column_count`) or CDC-native (`base_column_count + 2`), raising on anything else. This keeps the loader source-agnostic and catches genuine schema drift as a hard error instead of silently misaligning columns.
* **ADR-02 — XML flattened into two relational tables, not one denormalized table.** `CustomerMgmt.xml`'s nested `<Action> → <Customer> → <Account>` structure has no natural single-table representation without heavy redundancy (one Action can carry multiple Accounts). It's split into `bronze_customer_mgmt_event` (customer-level, one row per Action) and `bronze_customer_mgmt_account` (account-level, one row per nested Account), joined by `C_ID`.
* **ADR-03 — Audit files land in one unified reconciliation table.** All vendor `*_audit.csv` files across sources land in a single `bronze_source_audit` table rather than one table per source, so QA scripts have one place to check ingested counts against vendor-supplied totals.
* **ADR-04 — Staging file + `PUT`/`COPY INTO`, not row-by-row `INSERT`.** Snowflake's efficient path for file-based sources is bulk loading from a staged file, not per-row DML. Every loader normalizes its source into a local CSV matching `ff_bronze_csv`, then stages and bulk-loads it via `copy_into()` in `snowflake_client.py`.
* **ADR-05 — Row hash uses `blake2b(digest_size=8)`, not SHA-256 truncated to 8 bytes.** `_row_hash` is a dedup/lineage hash, not a security boundary, so there's no reason to pay for a cryptographic 256-bit digest and then discard 24 of its 32 bytes. `blake2b` with an 8-byte digest size is materially faster per call and runs once per ingested row across every table.
* **ADR-06 — Chunked staging writes, not full-buffer or per-row.** Writing a source's entire row set to the staging CSV in one shot risks out-of-memory on multi-GB fact tables (Trade, CashTransaction, DailyMarket); writing one `writerow()` call per row is memory-safe but pays Python call/formatting overhead with nothing amortized. `write_staging_csv` and `StreamingCsvWriter` both buffer formatted rows and flush with a single `writerows()` call every `DEFAULT_CHUNK_SIZE` (5000) rows, bounding peak memory to one chunk regardless of file size while batching the actual writes.
* **ADR-07 — Multi-table sources fan out via concurrently-open streaming writers, not per-table row lists.** FINWIRE interleaves CMP/SEC/FIN records in one file; `CustomerMgmt.xml` produces both event and account rows from one parse pass. Rather than accumulating a separate in-memory list per target table for the whole file, `finwire_loader.py` and `xml_loader.py` keep one `StreamingCsvWriter` open per target table and write each record to its table as soon as it's parsed, so no table's rows are buffered longer than the shared chunk size in ADR-06.
* **ADR-08 — XML parsed via `iterparse` with element clearing, not full-DOM `parse`.** `ET.parse()` loads the entire `CustomerMgmt.xml` tree into memory before any row can be produced. `xml_loader.py` uses `ET.iterparse(events=("end",))` and calls `elem.clear()` once each `<Action>` block has been flattened to CSV, so memory stays roughly proportional to one Action block rather than the whole document.
### Architecture Design Records — Bronze Layer DDL
 
These cover decisions baked into `ddl/bronze_schema.sql` itself, separate from the Python loader ADRs above.
 
* **ADR-09 — Uniform metadata envelope across every bronze table, no archetype exceptions.** Even the tiny, load-once Archetype A dimensions (`bronze_date`, `bronze_time`, `bronze_status_type`, …) carry the full `_batch_id`/`_source_file`/`_loaded_at`/`_row_hash` envelope, rather than special-casing "simple" tables to skip it. The cost is a handful of extra columns on small tables; the payoff is that every downstream reconciliation and lineage query works identically regardless of which archetype a table came from, with no per-table branching.
* **ADR-10 — CDC columns are always present and backfilled, never nullable-by-default.** Every Archetype B table carries `_cdc_flag`/`_cdc_dsn` as non-optional columns, with Batch1 rows backfilled to `('I', 0)` rather than left `NULL`. This means the standard silver-layer pattern (`QUALIFY ROW_NUMBER() OVER (... ORDER BY _cdc_dsn DESC) = 1`) works unmodified across all batches — no `NULL`-handling branch needed in every downstream model just because a row happened to arrive in Batch1.
* **ADR-11 — No `CLUSTER BY` at bronze-table creation time.** Snowflake micro-partitions automatically on load, and clustering keys cost ongoing reclustering credits once applied. Rather than guessing a clustering key up front, it's deferred to a per-table decision made later against real data volume and observed query pruning — `bronze_trade` is called out in the DDL comments as the most likely first candidate, but nothing is clustered speculatively.
* **ADR-12 — WatchHistory keeps `_cdc_dsn` even though it's insert-only.** Every WatchHistory row's `_cdc_flag` is `'I'` by spec (rows are only ever added), so `_cdc_dsn` isn't strictly load-bearing for this table today. It's kept anyway so WatchHistory's shape matches every other Archetype B table and can still support lineage/ordering queries later, rather than carving out a narrower schema for this one source.
* **ADR-13 — CashTransaction's CDC columns are left nullable, not backfilled.** Unlike Trade/HoldingHistory/etc., CDC presence for CashTransaction isn't confirmed against the source spec. Rather than assuming CDC-capability and hard-backfilling `('I', 0)` the way ADR-10 does elsewhere, `_cdc_flag`/`_cdc_dsn` are left nullable as an explicit "unconfirmed" signal, and `_row_hash` is documented in the DDL as the fallback dedup/QA mechanism if `_cdc_dsn` turns out to be unreliable or absent for this source.
* **ADR-14 — Archetype C (Prospect) retains every batch's full snapshot rather than overwriting or deduping at load time.** Each batch's complete re-extract lands as its own generation tagged by `_batch_id`, with no bronze-layer attempt to diff against the prior batch. "Current state" is resolved downstream by filtering to `_batch_id = MAX(_batch_id)`; storage is cheap at this volume and keeping every generation makes point-in-time comparison across batches trivial instead of requiring it to be reconstructed later.
* **ADR-15 — Control/reconciliation tables deliberately carry a reduced metadata envelope.** `bronze_batch_control` and `bronze_source_audit` get `_batch_id`/`_source_file`/`_loaded_at` where relevant but no `_row_hash` and no CDC columns, breaking from the uniform-envelope rule in ADR-09. These aren't business entities that downstream models dedup or resolve CDC state on — they're ingestion metadata and QA ground truth — so the columns that exist purely to support dedup/CDC resolution don't apply and aren't added just for consistency's sake.
* **ADR-16 — The staging file format is defined to exactly match the Python writer's output, not just be broadly CSV-compatible.** `ff_bronze_csv`'s `NULL_IF=('')` and `EMPTY_FIELD_AS_NULL=TRUE` line up with `format_csv_value()`'s empty-string-for-`NULL` convention; `DATE_FORMAT`/`TIMESTAMP_FORMAT` line up with its exact ISO rendering; `COMPRESSION=GZIP` matches the `AUTO_COMPRESS=TRUE` used in the `PUT` command in `copy_into()`. The DDL and the loader code are treated as one contract that changes together — a format change on one side without the other silently corrupts loads rather than erroring.
* **ADR-17 — Every `CREATE` statement uses `IF NOT EXISTS`.** The bronze DDL is written to be safely re-run in full (e.g. after adding one new table to the script) without a separate schema-diffing or migration step at this layer.
