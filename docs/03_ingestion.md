# Ingestion Layer

## Table of contents
- [Governing principle](#governing-principle)
- [Architecture overview](#architecture-overview)
- [Loader-to-source mapping](#loader-to-source-mapping)
- [Key design decisions, per loader](#key-design-decisions-per-loader)
- [Idempotency status (known gaps)](#idempotency-status-known-gaps)
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
  │
  ├─ loaders/delimited_loader.py   generic CSV/PSV loader (14 sources)
  ├─ loaders/finwire_loader.py     fixed-width, 3-record-type dispatcher
  ├─ loaders/xml_loader.py         nested XML flattener (CustomerMgmt.xml)
  └─ loaders/audit_loader.py       control/operational tables
```

`main.py`'s `run_batch()` drives one batch directory at a time:
`BatchDate.txt` → configured delimited sources → `CustomerMgmt.xml` →
`FINWIRE*` files → `*_audit.csv`. A source not present in the batch
directory is silently skipped — batch scope (Batch1 vs Batch2/3) is
expressed by which files exist on disk, not hardcoded in the script.

## Loader-to-source mapping

| Source | Loader | Notes |
|---|---|---|
| Date, Time, StatusType, TaxRate, Industry, TradeType, HR, Prospect, Account, Customer, Trade, HoldingHistory, WatchHistory, DailyMarket, CashTransaction, TradeHistory | `delimited_loader.load_delimited_source` | Driven by `config.DELIMITED_SOURCES`; one generic function handles all 15 |
| CustomerMgmt.xml | `xml_loader.load_customer_mgmt_xml` | Streaming XML parse, flattens to two tables |
| FINWIRE* | `finwire_loader.load_finwire_source` | Fixed-width, 3 interleaved record types dispatched by `RecType` |
| `*_audit.csv` | `audit_loader.load_audit_source` | Vendor reconciliation counts |
| BatchDate.txt | `audit_loader.load_batch_date` | Single-value control file |

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

## Idempotency status (known gaps)

Re-running the same batch (e.g. after an interrupted run) is **not fully
idempotent** end to end:

- Business-data tables that reach silver get protected there via
  `dedup_latest`/`_row_hash` collapse — a duplicate bronze load doesn't
  corrupt the silver result.
- **`bronze_batch_control` and `bronze_source_audit` are not deduplicated
  anywhere** (bronze or silver). A re-run appends a second row per
  `BatchID`. Since nothing currently joins on `bronze_batch_control` in a
  way that would silently fan out results, this hasn't caused an observed
  bug — but it's a real gap, not a resolved one, and should be closed
  before any downstream model joins on `BatchID` (see 02_bronze_design.md's
  fan-out discussion for why this matters).
- No pre-load check verifies whether a batch was already loaded before
  `run_batch()` starts — the operational responsibility of "don't run the
  same `--batch-id` twice" currently sits with whoever invokes `main.py`.

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

## Open items / not yet implemented

- `bronze_batch_control` / `bronze_source_audit` duplicate-load protection
  (see Idempotency section above).
- No automated reconciliation step yet compares `bronze_source_audit`'s
  vendor-supplied row counts against actual bronze row counts post-load —
  the table is populated, but nothing reads it back to validate.