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
  ├─ snowflake_client.py      connection + PUT/COPY INTO primitive
  ├─ common.py                shared casters, row-hash, streaming CSV writer
  ├─ config.py                declarative per-source column/type mapping
  │                           + ALL_BRONZE_TABLES (used by --force wipe)
  │
  ├─ loaders/delimited_loader.py   generic CSV/PSV loader (14 sources)
  ├─ loaders/finwire_loader.py     fixed-width, 3-record-type dispatcher
  ├─ loaders/xml_loader.py         nested XML flattener (CustomerMgmt.xml)
  └─ loaders/audit_loader.py       control/operational tables
```

`main.py`'s `run_batch()` drives one batch directory at a time:

1. Check `bronze_batch_control` for an existing row for this `_batch_id`.
   If found and `--force` was not passed, abort immediately (nothing else
   runs). If found and `--force` was passed, delete this batch's rows from
   every bronze table first (see
   [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)).
2. `BatchDate.txt` → `bronze_batch_control` (loaded first, deliberately —
   see below). **Required, not optional**: if the file is missing, the
   batch aborts immediately here rather than silently continuing without
   a control row (see
   [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)).
3. Configured delimited sources → `CustomerMgmt.xml` → `FINWIRE*` files →
   `*_audit.csv`.
4. Reconciliation check: compare `bronze_source_audit`'s expected row
   counts against what was actually loaded in this run (see
   [Reconciliation check](#reconciliation-check)).

A source not present in the batch directory is silently skipped — batch
scope (Batch1 vs Batch2/3) is expressed by which files exist on disk, not
hardcoded in the script. `BatchDate.txt` is the one exception to this
silent-skip convention (see below) — everything else is optional-by-file-
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

Sources like `Trade.txt`, `HoldingHistory.txt`, `WatchHistory.txt`,
`DailyMarket.txt` have **fewer columns in Batch1** than in Batch2/3 (no
`CDC_FLAG`/`CDC_DSN`). Rather than hardcode "Batch1 = no CDC" per source,
`_split_cdc()` inspects each line's actual field count:

- `field_count == base_column_count` → no CDC columns present; backfill
  `('I', 0)`.
- `field_count == base_column_count + 2` → CDC columns present; use them
  verbatim.
- anything else → hard `ValueError` (unexpected schema drift — fail loud
  rather than silently misalign columns).

This makes the loader source-file-driven rather than batch-number-driven:
it would handle a source's schema shift correctly even if the shift
happened on a different batch than expected, because it's detected from
the data itself, not from `batch_id`.

### `finwire_loader.py` — polymorphic fixed-width dispatch

One file contains three record types (`CMP`/`SEC`/`FIN`) distinguished by a
3-character `RecType` field after a 15-character `PTS` prefix. The loader
opens three `StreamingCsvWriter`s concurrently and routes each line to the
matching one by `RecType`, keeping memory bounded regardless of file size
(no need to buffer whole record-type groups before writing).

`CoNameOrCIK` (a single polymorphic trailing field in the source) is
resolved at parse time into two explicit, mutually exclusive columns
(`coname`, `cocik`) via `_resolve_co_name_or_cik()` — numeric-only content
is treated as a CIK, anything else as a company name, per spec.

An unrecognized `RecType` raises `ValueError` immediately rather than
silently dropping the line.

### `xml_loader.py` — streaming flatten of nested XML into two tables

`CustomerMgmt.xml`'s hierarchy (`Action` → `Customer` → `Account`, with
`Customer` further containing `Name`/`Address`/`ContactInfo`/`TaxInfo`) is
flattened into `bronze_mgmt_customer` (one row per Action-with-a-Customer)
and `bronze_mgmt_account` (one row per nested `Account` element, FK'd via
`C_ID`).

Three points confirmed against a real sample file + the published XSD
(not assumed):
1. `Name`/`Address`/`ContactInfo`/`TaxInfo` are genuinely nested elements
   under `Customer` (not flat attributes), each optional (`minOccurs="0"`).
2. Sparse `UPDCUST` payloads are real — a live sample showed `C_TIER`
   absent on an `UPDCUST` action, confirming the XSD's `minOccurs="0"`
   is authoritative over the spec's prose table wording ("Not empty"),
   which was misleading on this point.
3. `C_PHONE_1`/`C_PHONE_2`/`C_PHONE_3` are each a nested `PhoneNumber`
   element (`C_CTRY_CODE`/`C_AREA_CODE`/`C_LOCAL`/`C_EXT`), not flat
   fields — confirmed against a real `UPDCUST` sample showing this exact
   structure.

Every optional field is read via `.get()`/`.findtext()`, both of which
return `None` on absence, so a missing value becomes `NULL` on `COPY INTO`
with no special-casing needed. **No `COALESCE`/carry-forward logic runs
here** — reconstructing "current full record" from a sparse update stream
is explicitly deferred to silver.

Column naming for the phone fields (`c_ctry_1`, `c_area_1`, `c_local_1`,
`c_ext_1`, ...) mirrors `Customer.txt`'s flattened convention by decision,
so the two sources share a naming convention ahead of any silver-layer
unification of `bronze_customer`/`bronze_mgmt_customer`.

Uses `ET.iterparse(..., events=("end",))` with `action.clear()` after each
`Action` is flattened, keeping memory roughly proportional to one `Action`
block rather than the whole document.

### `audit_loader.py` — two small, simple control-file loaders

`load_batch_date()` reads a single value from `BatchDate.txt` and inserts
one row into `bronze_batch_control`. `load_audit_source()` reads a
vendor CSV (with defensive header-whitespace stripping — vendors
sometimes emit `"DataSet "` with a trailing space) into
`bronze_source_audit`. Both are declared `int`/`Decimal`/`date`-cast via
`_safe_cast`, same fault-tolerant pattern as every other loader.

### `common.py` — shared primitives

- `_safe_cast(raw, caster)`: strips whitespace, returns `None` on empty
  string or on any exception from `caster` — this is what makes malformed
  numeric/date fields degrade to `NULL` instead of aborting the batch.
  **Caveat**: this only works if `caster` can actually raise on bad input
  (e.g. `int`, `parse_date`) — a caster of plain `str` can never raise,
  so a corrupt value in a `str`-cast column will sail through unchanged
  to `COPY INTO`, where Snowflake's own type check is the only backstop.
- `compute_row_hash(values)`: BLAKE2b digest (8 bytes) over `"|"`-joined
  business column values, used as a QA/dedup/lineage signal across every
  loader.
- `StreamingCsvWriter` / `write_staging_csv`: bounded-memory CSV writers
  (5,000-row chunks) — the former for loaders that fan one input stream
  out to multiple target files concurrently (FINWIRE, XML), the latter
  for single-target loaders.
- `format_csv_value`: renders Python values into the exact string form
  Snowflake's `ff_bronze_csv` file format expects (empty string for
  `NULL`, `TRUE`/`FALSE` for booleans, millisecond-truncated timestamps).

### `snowflake_client.py` — the load primitive

`copy_into()` always passes an **explicit column list** to `COPY INTO`
(never positional-only), so a loader's output and the target table's
shape can't silently drift apart from each other even if one changes.
`ON_ERROR = 'ABORT_STATEMENT'` means a single malformed row that reaches
Snowflake's own type check aborts the whole file's load — this is why
`_safe_cast` degrading bad values to `NULL` upstream matters: it's the
main thing standing between a dirty row and a failed batch.

## Batch idempotency / re-ingestion

Every bronze table (business data and the two control tables) now carries
a consistently-named `_batch_id` column, and `run_batch()` checks
`bronze_batch_control` for that `_batch_id` before doing anything else:

- **No existing row, or `--force` not passed and none exists**: proceeds
  normally.
- **Existing row found, `--force` not passed**: `run_batch()` raises
  `RuntimeError` immediately and nothing is loaded. This is deliberately
  a hard failure, not a silent skip or silent overwrite — running the
  same `--batch-id` twice by accident should be loud.
- **Existing row found, `--force` passed**: `force_delete_batch()` deletes
  every row for that `_batch_id` from every table in
  `config.ALL_BRONZE_TABLES` (business tables and both control tables),
  wrapped in a single transaction so a mid-loop failure rolls the whole
  wipe back instead of leaving bronze partially cleaned. `run_batch()`
  then proceeds as a normal fresh load.

`bronze_batch_control` is loaded **first**, before any other source, so
that an interrupted run is correctly detected as "already attempted" on
the next invocation — rather than the next run silently reloading
everything a second time because the control row was never written.

**`BatchDate.txt` is required, not optional.** This entire safety net
above only works because the `exists` check reads `bronze_batch_control`
— nothing else. If `BatchDate.txt` were missing and the loader silently
skipped it (the old behavior), every *other* source in that batch would
still load successfully, but the batch would finish without ever writing
a row to `bronze_batch_control`. The next run for that `batch_id` — with
or without `--force` — would then see `exists = False`, conclude the
batch had never been attempted, and load everything again on top of the
first run's data with no warning and no dedup. Failing loud on a missing
`BatchDate.txt`, before any other source is touched, closes that gap at
its source instead of discovering it downstream as silent duplication.

Two things this design deliberately does **not** do:

- It does not distinguish "batch completed successfully" from "batch
  partially loaded, then failed" — both states are treated identically
  (existing row found → block unless `--force`). A partial failure is
  resolved by re-running with `--force`, which wipes and reloads the
  batch from scratch; there is no "resume from where it failed" path.
- `--force` deletes **all** bronze data for the batch, including business
  tables — not just the two control tables. This was a deliberate choice
  given that business tables are already protected downstream in silver
  by `dedup_latest`/`_row_hash`, so a stray duplicate load there was never
  the real risk; the goal here is avoiding storage bloat from repeated
  partial loads sitting in bronze indefinitely.

One consequence worth knowing: since the existing-batch check runs
*before* the `BatchDate.txt` check, a `--force` re-run against a batch
directory that's missing `BatchDate.txt` will first wipe all of that
batch's bronze rows, then raise — leaving the batch fully empty (not
partially loaded) rather than fully avoiding the wipe. This is a clean
failure state, not a data-corruption risk, but it does mean the wipe
happens before the missing-file problem is discovered, not after.

## Reconciliation check

At the end of `run_batch()`, after all sources (delimited, XML, FINWIRE,
audit files) have loaded, a `loaded_counts` dict — built incrementally
during ingestion, keyed by source filename stem — is compared against
`bronze_source_audit`'s vendor-supplied `RowCount` rows for this
`_batch_id`.

The comparison query pulls two shapes of expected count, unioned
together:
- Sources whose audit file has a single `*_RECORDS`-suffixed attribute
  (e.g. `WH_RECORDS` for `WatchHistory`) — that value is used directly as
  the expected row count.
- Sources with no such attribute (FINWIRE, Account, Customer,
  CustomerMgmt) — their positive-valued attributes are summed per
  `_source_file` instead (e.g. FINWIRE's `FW_CMP` + `FW_SEC` + `FW_FIN`),
  filtering out the negative `_DUP` counters so they don't offset the
  real total.

Additional notes:
- FINWIRE's three target tables (`bronze_finwire_cmp`/`sec`/`fin`) are
  summed into a single count under the FINWIRE file's stem, since the
  vendor audit file provides one `RowCount` for the whole FINWIRE file,
  not one per target table.
- CustomerMgmt.xml's two target tables (`bronze_mgmt_customer`,
  `bronze_mgmt_account`) are summed the same way, against the XML file's
  single audit entry.
- Matching between an audit row's `_source_file` (`"<filename>_audit.csv"`)
  and a `loaded_counts` key is done by stripping the `_audit.csv` suffix.

A mismatch (or an audit entry with no corresponding loaded source) prints
a `WARNING` line — it does **not** abort the batch. A mismatch can have a
legitimate explanation (e.g. intentionally skipped rows), so this is a
"someone should notice and investigate" signal, not a hard failure
condition. In practice this check has already caught a real discrepancy
during testing (an off-by-one in a locally-generated `WatchHistory`/
`Account` sample), confirming the comparison logic itself is sound.

## Failure handling

- **Delimited sources**: an unexpected field count on any line raises
  immediately (`_split_cdc`'s schema-drift guard) — the whole file's load
  aborts rather than silently misaligning columns for the rest of the
  file.
- **FINWIRE**: an unrecognized `RecType` raises immediately, same
  fail-loud stance.
- **Malformed individual values** (bad date, non-numeric int field, etc.):
  degrade to `NULL` via `_safe_cast`, do **not** abort the batch. This is
  a deliberate asymmetry — structural corruption (wrong shape) fails
  loud; value-level corruption (wrong content, right shape) is deferred
  to data-quality flags in silver.
- **XML**: an `Action` with no `Customer` element is skipped (`action.clear()`
  and `continue`) rather than raising — per spec, every `Action` has at
  least a related `Customer`, so this is a defensive guard against a
  malformed document, not an expected code path.
- **Missing `BatchDate.txt`**: raises `FileNotFoundError` immediately,
  before any other source in the batch is loaded. Unlike every other
  source (which is silently skipped if absent, since batch scope is
  file-presence-driven), `BatchDate.txt` is load-bearing for the
  idempotency check itself — see
  [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)
  for why a silent skip here is unsafe in a way that skipping, say,
  `Prospect.csv` is not.
- **Already-ingested batch without `--force`**: raises `RuntimeError`
  immediately, before any source is touched.

## Open items / not yet implemented

- No "resume from partial failure" path — recovering from an interrupted
  run means re-running the whole batch with `--force` (full wipe +
  reload), not resuming from the point of failure. See
  [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion).
- The reconciliation check (see above) warns on mismatch but has no
  structured output (e.g. a summary table or exit code) — it's
  `print`-only, meant for a human watching the run, not yet wired into
  any automated alerting.
- The existing-batch check runs before the `BatchDate.txt` check, so a
  `--force` re-run of a batch directory missing `BatchDate.txt` wipes
  bronze for that batch before discovering the missing file (see note at
  the end of
  [Batch idempotency / re-ingestion](#batch-idempotency--re-ingestion)).
  Not currently reordered, since the end state (empty, not partial) isn't
  unsafe — just worth knowing.