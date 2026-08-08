# Data Quality, Trust, and Consistency


## Table of Contents
1. [Bronze and Ingestion Layer](#bronze-and-ingestion-layer)
2. [Silver Layer](#silver-layer)
3. [Gold Layer](#gold-layer)


## Bronze and Ingestion Layer.
This layer covered three things before any data reached this point: **consistency** (the shape of every file is verified against what we expect — schema drift and column-list mismatches fail loud, not silently misaligned), **trust** (we don't just assume a batch loaded correctly — batch idempotency checks, a required control file, and reconciliation against vendor audit counts all confirm the numbers are real), and **data quality** (every value that fails to cast is kept, not dropped — `NULL` plus a structured reason in `_dq_errors`, so nothing disappears without a trace). Silver builds on that foundation...
### Problem 1 — Schema drift (wrong shape)

Some sources change shape between batches. Example: `Trade.txt`, `HoldingHistory.txt`, `WatchHistory.txt`, `DailyMarket.txt` have fewer columns in Batch1 (no `CDC_FLAG`/`CDC_DSN`) than in Batch2/3.


**How we handle it:**
- We don't hardcode "Batch1 = no CDC columns". Instead `_split_cdc()` counts the actual fields in each line:
    - field count == base columns → no CDC columns, backfill `('I', 0)`
    - field count == base columns + 2 → CDC columns present, use them
    - anything else → `ValueError`, stop loading that file
- Same for FINWIRE: an unrecognized `RecType` raises immediately.
- _Why detect from data, not batch number:_ this still works correctly even if a schema shift happens on a different batch than expected — we're reading the real shape of the line, not assuming it.
- _Why stop instead of continue:_ a wrong shape means our code misunderstood the file structure. Continuing would misalign every column after that point — silently wrong data is worse than a stopped file.

**Code reference:**\
`_split_cdc()` in `delimited_loader.py`\
`load_finwire_source()`'s unknown-`RecType` branch in `finwire_loader.py`.

### Problem 2 — Bad values (wrong content, right shape)
A field has the right shape (it's there, in the right position) but bad content — e.g. a date like `"122006"` that can't be parsed.

**How we handle it:**
- `_safe_cast(raw, caster, col_name)` never crashes. Empty string → `None`. Cast fails → `None` for the value, **plus** a structured error: `{column, raw_value, error_type, error_msg}`.
- The column becomes `NULL`, but we know exactly *why* — nothing is lost quietly.
- _Why continue instead of stop:_ bad values are normal, expected dirty data. Stopping the whole batch for one bad date is too strict — we fix what we can and keep moving.

**Code reference:** `_safe_cast()` in `common.py`, used by every loader.

### Problem 3 — Silent failures (old design)
The original `_safe_cast` just returned `None` on failure with no trace of what went wrong or which row/column it happened to. Bad data disappeared with zero evidence.

**How we handle it:**
- `_safe_cast` now returns `(value, error_info)` instead of just `value`. Every failed cast is captured.
- All per-column errors for one row are packed by `_pack_dq_errors()` into a single JSON list, saved in one column: `_dq_errors`.
    - Clean row → `_dq_errors` is `NULL`.
    - Dirty row → `_dq_errors` is a list you can open and read.
- Every bronze table carries this column, applied consistently across all four loaders (delimited, FINWIRE, XML, audit).
- The row still lands in bronze either way — we don't reject or quarantine it. Deciding what to do about a dirty row (fix, flag, reject) is Silver's job, not Bronze's. Bronze's job is: load everything, keep the evidence.

**Output example:**
```json
[
  {
    "column": "c_dob",
    "error_msg": "time data '1933k-04-19' does not match format '%Y-%m-%d'",
    "error_type": "ValueError",
    "raw_value": "1933k-04-19"
  }
]
```

**Code reference:** `_safe_cast()` and `_pack_dq_errors()` in `common.py`.

### Problem 4 — One control value that can't be allowed to fail silently
`BatchDate.txt`'s date drives the whole batch-idempotency system (see Problem 6 below). If this one value silently became `NULL`, every future run for that batch would be corrupted with no warning.

**How we handle it:**
- `asofdate` still goes through `_safe_cast` for a structured error, but here we don't soft-fail. `load_batch_date` **raises** if the cast fails, stopping the batch immediately.
- This is the one deliberate exception to Problem 2's "bad value = NULL and continue" rule.
- Because a row only ever lands in `bronze_batch_control` when `asofdate` is valid, the table is clean by construction — it doesn't even carry a `_dq_errors` column.

**Code reference:** `load_batch_date()` in `audit_loader.py`.

### Problem 5 — Loader output and table shape drifting apart
If a loader's column list and the target table's actual columns aren't kept in sync explicitly, they can silently drift (e.g. someone adds a column to one but not the other) and `COPY INTO` writes into the wrong columns.

**How we handle it:**
- `copy_into()` always takes an **explicit column list**, never positional-only.
- `ON_ERROR = 'ABORT_STATEMENT'`: if a row somehow still reaches Snowflake with a genuinely bad type, the whole file's load aborts loudly instead of partially loading.

**Code reference:** `copy_into()` in `snowflake_client.py`.


### Problem 6 — Running the same batch twice by accident
Without a check, re-running a batch (on purpose or by mistake) would duplicate every row in bronze.

**How we handle it:**
- Before loading anything, we check `bronze_batch_control` for an existing row with this `_batch_id`.
    - No existing row → proceed normally.
    - Existing row, no `--force` → stop immediately (`RuntimeError`). Loud on purpose — an accidental re-run should never silently duplicate data.
    - Existing row, `--force` passed → wipe every row for that `_batch_id` across all bronze tables (in one transaction, so a failed wipe rolls back cleanly), then load fresh.
- `bronze_batch_control` is loaded **first**, before any other source — so an interrupted run is correctly detected as "already attempted" on the next run, instead of silently reloading everything.

**Code reference:** `run_batch()` and `force_delete_batch()` in `main.py`.

> [!NOTE]
> Silver already protects against duplicate data via `dedup_latest/_row_hash` (see `04_silver.md`), so a stray re-ingested batch wouldn't corrupt downstream results either way. Bronze's own re-ingestion guard (this problem) exists for a different reason: preventing storage bloat from repeated full/partial batch loads sitting in bronze indefinitely — not as a second line of defense against bad data reaching silver.

### Problem 7 — A missing control file breaking the safety net silently
If `BatchDate.txt` is missing and we just skip it (like we do for every other optional source), the batch finishes without ever writing a row to `bronze_batch_control`. The next run then sees "no existing row", assumes nothing was ever loaded, and reloads everything on top — silent duplication with no warning.

**How we handle it:**
- `BatchDate.txt` is the **one required file**. Every other source is skipped silently if missing (batch scope is expressed by which files exist on disk). `BatchDate.txt` is not — its absence raises `FileNotFoundError` immediately, before any other source is touched.

**Code reference:** `run_batch()`'s `BatchDate.txt` check in `main.py`.

### Problem 8 — Trusting our own pipeline's numbers
A loader can run "successfully" and still have silently dropped or duplicated rows somewhere in transit.

**How we handle it:**
- We compare our own loaded row counts against the vendor-supplied `RowCount` values in `*_audit.csv` for the same batch.
- A mismatch prints a `WARNING` — it does **not** stop the batch, since a mismatch can have a legitimate explanation. This is a "someone should notice and investigate" signal, not an automatic failure.
- This is the classic pattern: check your numbers against an outside source, don't just trust your own pipeline. It already caught a real off-by-one during testing.

**Code reference:** the reconciliation query and comparison loop at the end of `run_batch()` in `main.py`.

### Metadata backbone
Every row in every bronze table carries: `_batch_id`, `_source_file`, `_loaded_at`, `_row_hash`, `_dq_errors`. Together these give us **lineage** (where a row came from), **dedup/QA** (`_row_hash` — is this row identical to one we've seen before), and **traceability** (`_dq_errors` — exactly what was dirty about this row and why).

**Code reference:** `compute_row_hash()` and `_pack_dq_errors()` in `common.py`.


## Silver Layer


## Gold Layer
