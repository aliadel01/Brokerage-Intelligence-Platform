# Ingestion Layer

## Table of contents
- [Ingestion Layer](#ingestion-layer)
  - [Table of contents](#table-of-contents)
  - [Governing principle](#governing-principle)
  - [Architecture overview](#architecture-overview)
  - [Loader-to-source mapping](#loader-to-source-mapping)
  - [Key design decisions, per loader](#key-design-decisions-per-loader)
    - [`delimited_loader.py` — schema-shift detection by field count](#delimited_loaderpy--schema-shift-detection-by-field-count)
    - [`finwire_loader.py` — polymorphic fixed-width dispatch](#finwire_loaderpy--polymorphic-fixed-width-dispatch)
    - [`xml_loader.py` — streaming flatten of nested XML into two tables](#xml_loaderpy--streaming-flatten-of-nested-xml-into-two-tables)
    - [`audit_loader.py` — two small, simple control-file loaders](#audit_loaderpy--two-small-simple-control-file-loaders)
    - [`common.py` — shared primitives](#commonpy--shared-primitives)
    - [`snowflake_client.py` — the load primitive](#snowflake_clientpy--the-load-primitive)
  - [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)
  - [Reconciliation check](#reconciliation-check)
  - [Failure handling](#failure-handling)
  - [Data quality (short note)](#data-quality-short-note)
  - [Open items / not yet implemented](#open-items--not-yet-implemented)

## Governing principle

Every loader in this layer exists to answer one question: **turn whatever
shape a source file arrives in into a staging CSV whose column order and
types match the target bronze table exactly**, then hand off to Snowflake's
bulk-load path (`PUT` + `COPY INTO`). No loader applies business logic,
deduplication-by-meaning, or cross-row reasoning — that's silver's job (see
`02_bronze_design.md`, Governing Principle). Ingestion's only
responsibilities are: **parse correctly, cast safely, preserve lineage,
load efficiently.**

## Architecture overview
```
main.py (CLI entry point)
│
├─ snowflake_client.py connection + PUT/COPY INTO primitive
├─ common.py shared casters, row-hash, streaming CSV writer
├─ config.py declarative per-source column/type mapping
│ + ALL_BRONZE_TABLES (used by --force wipe)
│
├─ loaders/delimited_loader.py generic CSV/PSV loader (14 sources)
├─ loaders/finwire_loader.py fixed-width, 3-record-type dispatcher
├─ loaders/xml_loader.py nested XML flattener (CustomerMgmt.xml)
└─ loaders/audit_loader.py control/operational tables
```
`main.py`'s `run_batch()` runs one batch directory at a time, step by step:

1. Check `bronze_batch_control` for an existing row for this `_batch_id`.
   If found and `--force` was not passed, stop right away (nothing else
   runs). If found and `--force` was passed, delete this batch's rows from
   every bronze table first (see
   [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)).
2. `BatchDate.txt` → `bronze_batch_control` (loaded first, on purpose —
   see below). **Required, not optional**: if the file is missing, the
   batch stops right here, instead of quietly continuing without
   a control row (see
   [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)).
3. Configured delimited sources → `CustomerMgmt.xml` → `FINWIRE*` files →
   `*_audit.csv`.
4. Reconciliation check: compare `bronze_source_audit`'s expected row
   counts against what was actually loaded in this run (see
   [Reconciliation check](#reconciliation-check)).

A source that isn't in the batch directory is just skipped, no error —
batch scope (Batch1 vs Batch2/3) is decided by which files exist on disk,
not hardcoded in the script. `BatchDate.txt` is the one exception to this
skip-if-missing rule (see below) — every other file is optional-by-file-
presence, `BatchDate.txt` is not.

## Loader-to-source mapping

| Source | Loader | Notes |
|---|---|---|
| Date, Time, StatusType, TaxRate, Industry, TradeType, HR, Prospect, Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket, CashTransaction, TradeHistory | `delimited_loader.load_delimited_source` | Driven by `config.DELIMITED_SOURCES`; one generic function handles all 15 |
| CustomerMgmt.xml | `xml_loader.load_customer_mgmt_xml` | Streaming XML parse, flattens to two tables |
| FINWIRE* | `finwire_loader.load_finwire_source` | Fixed-width, 3 interleaved record types dispatched by `RecType` |
| `*_audit.csv` | `audit_loader.load_audit_source` | Vendor reconciliation counts |
| BatchDate.txt | `audit_loader.load_batch_date` | Single-value control file; required (see above) |

## Key design decisions, per loader

### `delimited_loader.py` — schema-shift detection by field count

**What it does:** reads any pipe/comma-separated source file, one line at
a time. For each line, it works out if `CDC_FLAG`/`CDC_DSN` columns are
there or not, casts every business field with `_safe_cast`, computes a
row hash, and writes the row to a staging CSV. At the end it calls
`copy_into()` to load the staging CSV into the target bronze table.

Sources like `Trade.txt`, `HoldingHistory.txt`, `WatchHistory.txt`,
`DailyMarket.txt` have **fewer columns in Batch1** than in Batch2/3 (no
`CDC_FLAG`/`CDC_DSN`). Instead of hardcoding "Batch1 = no CDC", `_split_cdc()`
looks at each line's actual field count:

- `field_count == base_column_count` → no CDC columns present; fill in
  `('I', 0)` as default.
- `field_count == base_column_count + 2` → CDC columns present; use them
  as-is.
- anything else → hard `ValueError` (unexpected schema drift — stop loud
  instead of quietly misaligning columns).

This makes the loader driven by the source file itself, not by the batch
number: it would still handle a source's schema shift correctly even if
the shift happened on a different batch than expected, because it's read
from the data, not assumed from `batch_id`.

### `finwire_loader.py` — polymorphic fixed-width dispatch

**What it does:** reads FINWIRE's fixed-width lines one at a time. Each
line starts with a `PTS` timestamp (15 chars) and a `RecType` (3 chars:
`CMP`/`SEC`/`FIN`). Based on `RecType`, the line's remaining fields are
sliced by fixed width, cast field by field, and routed to one of three
`StreamingCsvWriter`s open at the same time — so memory use stays low no
matter how big the file is. At the end, each of the three staging files
(CMP/SEC/FIN) is loaded to its own bronze table.

`CoNameOrCIK` (one field in the source that can mean two different
things) is worked out at parse time into two separate columns
(`coname`, `cocik`) via `_resolve_co_name_or_cik()` — numbers-only means
CIK, anything else means company name, per spec.

An unrecognized `RecType` raises `ValueError` right away instead of
quietly dropping the line.

### `xml_loader.py` — streaming flatten of nested XML into two tables

**What it does:** streams through `CustomerMgmt.xml` one `Action` element
at a time (using `ET.iterparse`, so the whole file is never loaded into
memory at once). For each `Action`, it pulls out the `Customer` fields
(name, address, contact info, tax info, phone numbers) and writes one row
to the customer staging file. Then, for every nested `Account` inside
that `Customer`, it writes one more row to the account staging file. Both
staging files are loaded into their bronze tables (`bronze_mgmt_customer`,
`bronze_mgmt_account`) at the end.

`CustomerMgmt.xml`'s hierarchy (`Action` → `Customer` → `Account`, with
`Customer` also containing `Name`/`Address`/`ContactInfo`/`TaxInfo`) is
flattened into `bronze_mgmt_customer` (one row per Action-with-a-Customer)
and `bronze_mgmt_account` (one row per nested `Account` element, linked
via `C_ID`).

Three points confirmed against a real sample file + the published XSD
(not assumed):
1. `Name`/`Address`/`ContactInfo`/`TaxInfo` are genuinely nested elements
   under `Customer` (not flat attributes), each optional (`minOccurs="0"`).
2. Sparse `UPDCUST` payloads are real — a live sample showed `C_TIER`
   absent on an `UPDCUST` action, confirming the XSD's `minOccurs="0"`
   is the correct rule, not the spec's prose table wording ("Not empty"),
   which was misleading on this point.
3. `C_PHONE_1`/`C_PHONE_2`/`C_PHONE_3` are each a nested `PhoneNumber`
   element (`C_CTRY_CODE`/`C_AREA_CODE`/`C_LOCAL`/`C_EXT`), not flat
   fields — confirmed against a real `UPDCUST` sample showing this exact
   structure.

Every optional field is read via `.get()`/`.findtext()`, both of which
return `None` when absent, so a missing value becomes `NULL` on `COPY INTO`
with no extra handling needed. **No `COALESCE`/carry-forward logic runs
here** — building "current full record" from a sparse update stream
is left to silver on purpose.

Column naming for the phone fields (`c_ctry_1`, `c_area_1`, `c_local_1`,
`c_ext_1`, ...) matches `Customer.txt`'s flattened naming by choice, so
the two sources use the same naming ahead of any silver-layer
unification of `bronze_customer`/`bronze_mgmt_customer`.

Uses `ET.iterparse(..., events=("end",))` with `action.clear()` after each
`Action` is flattened, so memory use stays close to one `Action` block
instead of the whole document.

### `audit_loader.py` — two small, simple control-file loaders

**What it does:** two independent functions in one file.

`load_audit_source()` reads a vendor CSV (with header-whitespace
stripping — vendors sometimes send `"DataSet "` with a trailing space)
row by row, casts each field with `_safe_cast`, and loads it into
`bronze_source_audit`.

`load_batch_date()` reads the one line inside `BatchDate.txt`, casts it as
a date, and inserts one row into `bronze_batch_control`. This function is
different from every other loader in the whole codebase: on a bad cast, it
**raises** instead of loading a `NULL` and moving on. Why: this date drives
the whole batch-idempotency check (see
[Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)), so a
`NULL` here would quietly break that safety net for every future run of this
batch. See `06_data_quality.md` (Ingestion section, Problem 4) for the full
reasoning.

### `common.py` — shared primitives

- `_safe_cast(raw, caster, col_name)`: strips whitespace, returns `None`
  on empty string or on any exception from `caster` — this is what lets
  bad numeric/date fields become `NULL` instead of stopping the batch. It
  also returns error details when the cast fails, so nothing is lost
  silently — see `06_data_quality.md` (Ingestion section) for the full
  DQ design; this doc stays focused on how each file works, not on DQ
  itself.
  **Caveat**: this only works if `caster` can actually raise on bad input
  (e.g. `int`, `parse_date`) — a caster of plain `str` can never raise,
  so a corrupt value in a `str`-cast column will pass through unchanged
  to `COPY INTO`, where Snowflake's own type check is the only backstop.
- `compute_row_hash(values)`: BLAKE2b digest (8 bytes) over `"|"`-joined
  business column values, used as a QA/dedup/lineage signal across every
  loader.
- `StreamingCsvWriter` / `write_staging_csv`: bounded-memory CSV writers
  (5,000-row chunks) — the former for loaders that fan one input stream
  out to multiple target files at the same time (FINWIRE, XML), the latter
  for single-target loaders.
- `format_csv_value`: turns Python values into the exact string form
  Snowflake's `ff_bronze_csv` file format expects (empty string for
  `NULL`, `TRUE`/`FALSE` for booleans, millisecond-truncated timestamps).

### `snowflake_client.py` — the load primitive

**What it does:** one function, `copy_into()`, used by every loader at the
end of its work. It `PUT`s the staging CSV to the Snowflake internal stage,
then runs `COPY INTO` with an **explicit column list** (never
positional-only), so a loader's output and the target table's shape can't
quietly drift apart from each other even if one side changes.
`ON_ERROR = 'ABORT_STATEMENT'` means a single row that reaches Snowflake's
own type check with a genuinely bad value stops the whole file's load —
this is why casting bad values to `NULL` upstream (via `_safe_cast`)
matters: it's the main thing standing between a dirty row and a stopped
batch.

## Batch idempotency / re-ingestion
Every bronze table (business data and the two control tables) now carries
a `_batch_id` column, and `run_batch()` checks `bronze_batch_control` for
that `_batch_id` before doing anything else — but only **after** confirming
`BatchDate.txt` exists (see below):

- **No existing row, or `--force` not passed and none exists**: proceeds
  normally.
- **Existing row found, `--force` not passed**: `run_batch()` raises
  `RuntimeError` right away and nothing is loaded. This is on purpose —
  running the same `--batch-id` twice by accident should be loud, not
  silent.
- **Existing row found, `--force` passed**: `force_delete_batch()` deletes
  every row for that `_batch_id` from every table in
  `config.ALL_BRONZE_TABLES` (business tables and both control tables),
  inside one transaction so a mid-loop failure rolls the whole wipe back
  instead of leaving bronze half-cleaned. `run_batch()` then proceeds as a
  normal fresh load.

`bronze_batch_control` is loaded **first among the actual sources**, before
any other source, so an interrupted run is correctly seen as "already
attempted" on the next run — instead of the next run quietly reloading
everything again because the control row was never written.

**`BatchDate.txt`'s existence is checked at the very top of `run_batch()`,
before the idempotency check and before `--force` can touch anything.**
This whole safety check depends on that file being loadable, so its
presence is confirmed first: `run_batch()` raises `FileNotFoundError`
immediately if it's missing, before querying `bronze_batch_control` at
all and before any `--force` deletion could run. Unlike every other source
(which is silently skipped if missing, since batch scope is
file-presence-driven), `BatchDate.txt` is required — the old behavior
skipped it silently, which meant every *other* source in that batch would
still load fine, but the batch would finish without ever writing a row to
`bronze_batch_control`. The next run for that `batch_id` — with or without
`--force` — would then see `exists = False`, think the batch was never
attempted, and load everything again on top of the first run's data, with
no warning and no dedup. Checking the file's existence before the
idempotency check and before `--force` closes that gap at the source.

Two things this design does **not** do, on purpose:

- It does not tell apart "batch completed successfully" from "batch
  partially loaded, then failed" — both look the same (existing row found
  → block unless `--force`). A partial failure is fixed by re-running with
  `--force`, which wipes and reloads the batch from scratch; there is no
  "resume from where it failed" path.
- `--force` deletes **all** bronze data for the batch, including business
  tables — not just the two control tables. This was a deliberate choice:
  business tables are already protected downstream in silver by
  `dedup_latest`/`_row_hash`, so a stray duplicate load there was never
  the real risk. The goal here is avoiding storage bloat from repeated
  partial loads sitting in bronze forever.

Because the `BatchDate.txt` existence check now runs before the
idempotency check and before `--force`, the earlier edge case — a
`--force` re-run wiping bronze before discovering a missing
`BatchDate.txt` — can no longer happen. A missing file now blocks the
batch before any deletion or loading is attempted.

## Reconciliation check

At the end of `run_batch()`, after all sources (delimited, XML, FINWIRE,
audit files) have loaded, a `loaded_counts` dict — built step by step
during ingestion, keyed by source filename stem — is compared against
`bronze_source_audit`'s vendor-supplied `RowCount` rows for this
`_batch_id`.

The comparison query pulls two shapes of expected count, combined
together:
- Sources whose audit file has a single `*_RECORDS`-suffixed attribute
  (e.g. `WH_RECORDS` for `WatchHistory`) — that value is used directly as
  the expected row count.
- Sources with no such attribute (FINWIRE, Account, Customer,
  CustomerMgmt) — their positive-valued attributes are summed per
  `_source_file` instead (e.g. FINWIRE's `FW_CMP` + `FW_SEC` + `FW_FIN`),
  ignoring the negative `_DUP` counters so they don't offset the real
  total.

Additional notes:
- FINWIRE's three target tables (`bronze_finwire_cmp`/`sec`/`fin`) are
  summed into one count under the FINWIRE file's stem, since the vendor
  audit file gives one `RowCount` for the whole FINWIRE file, not one per
  target table.
- CustomerMgmt.xml's two target tables (`bronze_mgmt_customer`,
  `bronze_mgmt_account`) are summed the same way, against the XML file's
  single audit entry.
- Matching between an audit row's `_source_file` (`"<filename>_audit.csv"`)
  and a `loaded_counts` key is done by removing the `_audit.csv` suffix.

A mismatch (or an audit entry with no matching loaded source) prints a
`WARNING` line — it does **not** stop the batch. A mismatch can have a
legitimate reason (e.g. rows skipped on purpose), so this is a "someone
should notice and check" signal, not an automatic failure. In testing,
this check already caught a real off-by-one in a locally-generated
`WatchHistory`/`Account` sample, which confirms the comparison logic
itself works correctly.

## Failure handling
- **Delimited sources**: an unexpected field count on any line raises
  right away (`_split_cdc`'s schema-drift guard) — the whole file's load
  stops instead of quietly misaligning columns for the rest of the file.
- **FINWIRE**: an unrecognized `RecType` raises right away, same stop-loud
  approach.
- **Malformed individual values** (bad date, non-numeric int field, etc.):
  become `NULL` via `_safe_cast`, do **not** stop the batch. This is on
  purpose — wrong shape means our code misread the file's structure and
  must stop; wrong content in the right shape is normal dirty data, so we
  keep going. Full DQ handling of this case (how the error is recorded,
  the one exception for `asofdate`) is covered in `06_data_quality.md`
  (Ingestion section) — kept out of this doc to avoid repeating it twice.
- **XML**: an `Action` with no `Customer` element is skipped (`action.clear()`
  and `continue`) instead of raising — per spec, every `Action` should have
  a related `Customer`, so this is a defensive guard against a broken
  document, not something expected to normally happen.
- **Missing `BatchDate.txt`**: raises `FileNotFoundError` immediately, at
  the very top of `run_batch()` — before the batch-directory existence
  check's sibling checks, before the `bronze_batch_control` idempotency
  query, and before any `--force` deletion could run. Unlike every other
  source (which is silently skipped if missing, since batch scope is
  file-presence-driven), `BatchDate.txt` is load-bearing for the
  idempotency check itself — see
  [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)
  for why a silent skip here is unsafe in a way that skipping, say,
  `Prospect.csv` is not.
- **Already-ingested batch without `--force`**: raises `RuntimeError`
  right away, before any source is touched.****

## Data quality (short note)

This doc covers how each loader works — file shape, parsing, schema
detection, and load mechanics. The full data quality design (how bad
values are caught, recorded, and kept traceable via `_dq_errors`; the one
exception for `BatchDate.txt`'s `asofdate`) lives in `06_data_quality.md`,
Ingestion section — see that doc for the complete picture.

## Open items / not yet implemented

- No "resume from partial failure" path — recovering from an interrupted
  run means re-running the whole batch with `--force` (full wipe +
  reload), not resuming from the point of failure. See
  [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion).
- The reconciliation check (see above) warns on mismatch but has no
  structured output (e.g. a summary table or exit code) — it's
  `print`-only, meant for a human watching the run, not yet wired into
  any automated alerting.
- No Logs implementation 