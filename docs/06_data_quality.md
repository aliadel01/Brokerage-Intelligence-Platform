# Data Quality, Trust, and Consistency


## Table of Contents
- [Data Quality, Trust, and Consistency](#data-quality-trust-and-consistency)
  - [Table of Contents](#table-of-contents)
  - [Bronze and Ingestion Layer.](#bronze-and-ingestion-layer)
    - [Problem 1 — Schema drift (wrong shape)](#problem-1--schema-drift-wrong-shape)
    - [Problem 2 — Bad values (wrong content, right shape)](#problem-2--bad-values-wrong-content-right-shape)
    - [Problem 3 — Silent failures (old design)](#problem-3--silent-failures-old-design)
    - [Problem 4 — One control value that can't be allowed to fail silently](#problem-4--one-control-value-that-cant-be-allowed-to-fail-silently)
    - [Problem 5 — Loader output and table shape drifting apart](#problem-5--loader-output-and-table-shape-drifting-apart)
    - [Problem 6 — Running the same batch twice by accident](#problem-6--running-the-same-batch-twice-by-accident)
    - [Problem 7 — A missing control file breaking the safety net silently](#problem-7--a-missing-control-file-breaking-the-safety-net-silently)
    - [Problem 8 — Trusting our own pipeline's numbers](#problem-8--trusting-our-own-pipelines-numbers)
    - [Problem 9 — DQ-as-Control on Ingestion](#problem-9--dq-as-control-on-ingestion)
    - [Data Freshness](#data-freshness)
    - [Metadata backbone](#metadata-backbone)
  - [Silver Layer](#silver-layer)
    - [0. These points are covered in detail in `04_silver.md`, but here is a summary.](#0-these-points-are-covered-in-detail-in-04_silvermd-but-here-is-a-summary)
    - [1. dbt generic tests \& Referential integrity](#1-dbt-generic-tests--referential-integrity)
    - [2. SCD2-specific checks (account/customer)](#2-scd2-specific-checks-accountcustomer)
    - [3. Reconciliation — bronze row count vs. silver row count](#3-reconciliation--bronze-row-count-vs-silver-row-count)
    - [4. Business-rule tests (custom singular tests)](#4-business-rule-tests-custom-singular-tests)
      - [Problem 1 — Illegal trade status state transitions](#problem-1--illegal-trade-status-state-transitions)
      - [Problem 2 — False versioning in tracked SCD Type 2 entities](#problem-2--false-versioning-in-tracked-scd-type-2-entities)
    - [Store failed test results — how it works](#store-failed-test-results--how-it-works)
      - [1. `governance.dbt_test_results` (DDL)](#1-governancedbt_test_results-ddl)
      - [2. `macros/log_test_results.sql` — line by line](#2-macroslog_test_resultssql--line-by-line)
      - [3. `dbt_project.yml` wiring — what each new line does](#3-dbt_projectyml-wiring--what-each-new-line-does)
      - [4. How to use it day to day](#4-how-to-use-it-day-to-day)
    - [ADR: Custom test-result logging over Elementary package](#adr-custom-test-result-logging-over-elementary-package)
  - [Gold Layer](#gold-layer)
    - [Unknown member rows — fact FK columns never NULL](#unknown-member-rows--fact-fk-columns-never-null)
    - [Dimension attribute NULL fill](#dimension-attribute-null-fill)
    - [](#)


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

### Problem 9 — DQ-as-Control on Ingestion
You can see the full documentation of this phase in `07_governance.md` — `DQ-as-Control on Ingestion` section.

### Data Freshness
Standardizes ingestion monitoring via dbt source freshness using a 24-hour warning threshold on _loaded_at for incremental tables (Batch 2 & 3), while explicitly omitting static historical tables (Batch 1) to eliminate false positives.

### Metadata backbone
Every row in every bronze table carries: `_batch_id`, `_source_file`, `_loaded_at`, `_row_hash`, `_dq_errors`. Together these give us **lineage** (where a row came from), **dedup/QA** (`_row_hash` — is this row identical to one we've seen before), and **traceability** (`_dq_errors` — exactly what was dirty about this row and why).

**Code reference:** `compute_row_hash()` and `_pack_dq_errors()` in `common.py`.


## Silver Layer

### 0. These points are covered in detail in `04_silver.md`, but here is a summary.
- **Deduplication & Uniqueness**: Standardized deduplication logic using a unified dedup_latest macro. Implemented two core strategies: State-tracking to keep the latest entity state (e.g., silver_trade), and Append-only to eliminate exact duplicate records while retaining distinct version events (e.g., silver_account, silver_customer). Resolves dual-source overlap to prevent data multiplication.  
- **Data Completeness & Forward-Fill**: Handles partial CDC payloads using LAST_VALUE(...) IGNORE NULLS windowing. Executes forward-fill across the full chronological event sequence prior to the daily collapse step, preventing historical attributes from reverting to NULL during aggregation.  

- **Standardization & Semantic Consistency**: Unifies heterogeneous source vocabularies (flat-file CDC and XML) into standardized domains (e.g., mapping status_id to 'ACTV'/'INAC' and cdc_flag to 'I'/'U'). Filters non-entity noise events to maintain clean downstream structures. 

- **Data Integrity & Fan Trap Prevention**: Decoupled current state from lifecycle transitions (ADR-002) by splitting Trade into silver_trade (latest state) and silver_trade_history (transition history). Structurally prevents Kimball Fan Traps in the Gold layer, avoiding false multiplication of financial metrics (e.g., SUM(commission)) during aggregation.
-  **Determinism & Lineage**: Enforced deterministic processing via an explicit composite ordering key (_batch_id, action_ts, _cdc_dsn, _loaded_at) to break ties and guarantee idempotent pipeline execution. Implemented explicit lineage tracking using a _source_model column rather than relying on implicit assumptions like checking _batch_id = 1.
### 1. dbt generic tests & Referential integrity
Implemented core dbt testing across the Silver layer using `not_null`, `unique`, `unique_combination_of_columns`, and `accepted_values` for schema and metadata integrity. Configured `relationships` tests on key dependencies (`silver_trade`, `silver_account`, `silver_holding_history`, `silver_trade_history`) with severities set to **warn** instead of **error** to log orphan records without breaking the pipeline.

- Each silver model has a PK which it can be Business Primary Key, Surrogate Key, _row_hash or Composite Key. The PK is enforced via `unique` and `not_null` tests.

- severity: error $\rightarrow$ not_null, unique, unique_combination_of_columns.- 
- severity: warn $\rightarrow$ accepted_values, relationships (Orphan/Late-arriving dimension tolerance).

### 2. SCD2-specific checks (account/customer)
- `assert_scd2_active_flag_integrity`: Validates SCD Type 2 active flag integrity, asserting that each entity key (account_id / customer_id) resolves to exactly one record with is_current = true.
- `assert_no_overlapping_date_ranges`: Guarantees SCD Type 2 timeline boundary validity, flagging any record where valid_from_date overlaps with the preceding version's valid_to_date.
- `assert_scd2_date_continuity`: Enforces strict date chain continuity between adjacent state versions, verifying that $valid\_to\_date_{N} = valid\_from\_date_{N+1} - 1\text{ day}$ with zero gaps.
- `assert_forward_fill_sanity`: Verifies CDC attribute propagation consistency, ensuring tracked business columns do not regression-drop to NULL across versions once initially populated.

### 3. Reconciliation — bronze row count vs. silver row count

**Problem:** dedup, collapse, and filter logic inside silver models
(`dedup_latest`, SCD2 day-collapse, `where t_id is not null`, etc.) can
silently drop more or fewer rows than intended. No test in `04_silver.md`
catches a dedup step that ate too much — or too little.

**How we handle it:** `run_silver_reconciliation_checks()`
(`macros/run_silver_reconciliation_checks.sql`) runs on every `dbt build`
via `on-run-end`. Per silver model, it compares bronze row count against
silver row count **per `_batch_id`**, and logs one row per batch into
`governance.dq_audit_log` — same table, same pattern as the ingestion-layer
reconciliation check (`06_data_quality.md` Problem 8, `07_governance.md`
DQ-as-Control).

- `check_type = 'silver_reconciliation'`
- `severity = 'PASS'` if delta within threshold, else `'WARNING'` —
  **never fatal**, same reasoning as bronze reconciliation: a mismatch may
  be expected behavior (dedup, day-collapse), not a bug, so a human
  reviews it rather than the build failing automatically.
- Threshold is **per model**, not global, because expected delta varies
  hugely by archetype:

| Model type | Example | Threshold | Why |
|---|---|---|---|
| Archetype A pass-through | `silver_hr` | 1% | Should be near-exact 1:1 |
| Quasi-CDC append-only | `silver_holding_history` etc. | 2% | Only exact-dupe rows drop |
| Prospect (SCD1 snapshot) | `silver_prospect` | 10% | Intra-batch dupes collapse |
| Trade history (dual union, batch1 excluded from one side) | `silver_trade_history` | 30% | Structural, filtered union |
| Account / Customer (dual-source, day-collapse, tracked-col filter) | `silver_account`, `silver_customer` | 50% | Heavy collapse by design |
| Trade (many events -> 1 row per `trade_id`) | `silver_trade` | 80% | Collapse is the entire point of the model |

> [!NOTE]
> These thresholds are starting defaults, not confirmed business
> requirements — nobody has specified an official "acceptable delta %"
> yet. Revisit once real batch volumes are known; a fixed % can hide a
> real problem at high volume or false-alarm at low volume. Until then,
> `dq_audit_log` keeps every raw count queryable regardless of threshold,
> so nothing is lost even if a threshold turns out wrong.

`governance.dq_audit_log` already exists in prod (created by the
ingestion-layer `log_dq_event()` — see `07_governance.md`); no new DDL
needed for this check.

**Code reference:** `macros/log_silver_reconciliation.sql` (generic
logger), `macros/run_silver_reconciliation_checks.sql` (per-model calls),
wired via `on-run-end` in `dbt_project.yml`.

 
### 4. Business-rule tests (custom singular tests)

#### Problem 1 — Illegal trade status state transitions

A trade moves through a deterministic lifecycle via a state machine (e.g., Pending $\rightarrow$ Submitted $\rightarrow$ Completed). Race conditions in event processing or upstream source corruption can ingest impossible state jumps (e.g., `CMPT` before `SBMT`, or transitioning out of `CMPT` back to `PNDG`), corrupting `silver_trade_history` and downstream financial metrics.

**How we handle it:**

* `assert_trade_status_order.sql` evaluates consecutive status pairs per `trade_id` ordered chronologically using `LAG()`.
* Transitions are validated against an explicit whitelist of legal TPC-DI state pairs:
* `('PNDG','SBMT')`, `('PNDG','CANC')`
* `('SBMT','CMPT')`, `('SBMT','CANC')`
* `('CMPT','INAC')`, `('CANC','INAC')`


* Any record where `prev_status_id IS NOT NULL` and the transition pair `(prev_status_id, status_id)` is not in the whitelist is returned as an invalid transition error.

**Code reference:** `tests/silver_trade_history/assert_trade_status_order.sql`.

#### Problem 2 — False versioning in tracked SCD Type 2 entities

When updating `silver_account` or `silver_customer`, flaws in incremental merge or day-collapse logic can generate a new row version even when none of the explicitly tracked business attributes have changed (e.g., triggered by irrelevant metadata fields or redundant batch re-runs). This creates "false versions," inflating table size and skewing temporal reporting.

**How we handle it:**

* `assert_account_versions_only_on_tracked_change.sql` and `assert_customer_versions_only_on_tracked_change.sql` compare adjacent entity states using `LAG()` ordered by the deterministic key sequence `(_batch_id, action_ts, _cdc_dsn, _loaded_at)`.
* The tests verify that a new version exists **if and only if** at least one tracked business column has changed:
* **Account tracked columns:** `status_id`, `account_name`.
* **Customer tracked columns:** `status_id`, `last_name`, `first_name`, `tier`, `address_line1`, `city`, `state_province`, `country`, `primary_email`.


* Evaluating `(current_cols) IS NOT DISTINCT FROM (prev_cols)` catches rows where a version was spawned without a tracked attribute change, failing the build immediately.

**Code reference:**

`tests/silver_account/assert_account_versions_only_on_tracked_change.sql`
`tests/silver_customer/assert_customer_versions_only_on_tracked_change.sql`.

### Store failed test results — how it works

Two mechanisms, working together:

1. **`store_failures` (dbt native)** — physically saves the failing ROWS
   of every failed test as a real table in Snowflake.
2. **`log_test_results()` (custom, same pattern as `dq_audit_log`)** —
   saves a SUMMARY row (which test, pass/fail, how many rows, when) for
   EVERY test run, every invocation, whether it failed or not.

Together: `store_failures` = the evidence. `dbt_test_results` = the index
you query to find out what to look at.

---

#### 1. `governance.dbt_test_results` (DDL)

New table, separate from `dq_audit_log` on purpose — `dq_audit_log` is
batch-scoped (`batch_id NOT NULL`), tests are not. One table per evidence
type, matching your existing `07_governance.md` philosophy.

| Column | Meaning |
|---|---|
| `invocation_id` | UUID dbt generates once per `dbt build`/`dbt test` command. Groups all tests from the same run. |
| `run_started_at` | Timestamp the invocation started. |
| `test_name` | dbt's unique id for the test, e.g. `unique_silver_trade_trade_id`. |
| `model_name` | Which model the test is attached to, e.g. `silver_trade`. |
| `status` | `pass` / `fail` / `warn` / `error` / `skipped`. |
| `severity` | `error` or `warn`, from the test's own config (`config: {severity: warn}` in your `.yml`). |
| `failures` | How many rows failed the test. |
| `execution_time` | Seconds the test took. |
| `message` | dbt's own failure message text. |

Run this once to create it.

---

#### 2. `macros/log_test_results.sql` — line by line

```jinja
{% macro log_test_results(results) %}
```
Defines the macro, takes one argument: `results`.

```jinja
  {% if execute %}
```
dbt compiles every macro twice — once to just parse/plan (`execute =
false`), once for real (`execute = true`). Without this guard, the macro
body would try to run SQL during the parse pass and fail. Standard dbt
guard, you'll see it in every macro that runs a query.

```jinja
    {% set test_rows = [] %}
```
Empty list — we'll build up one SQL `VALUES (...)` tuple per test result
here, then insert them all in one statement instead of one `INSERT` per
test (much faster).

```jinja
    {% for r in results %}
```
`results` is a **dbt built-in** — when you put a macro call in
`on-run-end`, dbt automatically hands you a list of every node it just
ran (models, tests, seeds — everything), each one a `Result` object. You
don't create this list yourself, dbt gives it to you.

```jinja
      {% if r.node.resource_type == 'test' %}
```
`results` contains models too — we only care about test nodes here, so
skip anything that isn't one.

```jinja
        {% set model_name = r.node.attached_node if r.node.attached_node else r.node.depends_on.nodes[0] %}
```
Gets the model this test belongs to. Most tests (`not_null`, `unique`,
etc. defined under a model's `columns:` in `.yml`) have `attached_node`
set directly. Singular tests (a raw `.sql` file in `tests/`) don't have
that attribute populated the same way, so we fall back to the first node
it `depends_on` — normally the model it queries.

```jinja
        {% set severity = r.node.config.severity | lower if r.node.config is defined else 'error' %}
```
Reads the test's configured severity (`error` is dbt's default if none
was set). Lowercased for consistent storage.

```jinja
        {% set msg = (r.message or '') | replace("'", "''") %}
```
dbt's failure message, e.g. `"Got 3 results, configured to fail if != 0"`.
`replace("'", "''")` escapes single quotes — without this, a message
containing an apostrophe would break the SQL string and crash the insert.

```jinja
        {% set row %}
          ('{{ invocation_id }}', '{{ run_started_at }}', '{{ r.node.name }}',
           '{{ model_name }}', '{{ r.status }}', '{{ severity }}',
           {{ r.failures if r.failures is not none else 'null' }},
           {{ r.execution_time }}, '{{ msg }}')
        {% endset %}
```
Builds one SQL tuple `('val1', 'val2', ...)` matching the table's column
order exactly. `invocation_id` and `run_started_at` are **also** dbt
built-ins, available anywhere in a run — no need to pass them in.
`r.failures` can be `None` (e.g. for a test that errored before it could
count rows) — the inline `if/else` writes SQL `null` instead of Python
`None` (which would break the SQL).

```jinja
        {% do test_rows.append(row) %}
      {% endif %}
    {% endfor %}
```
Adds the tuple to the list, closes the `if`, closes the `for` loop — one
tuple built per test node.

```jinja
    {% if test_rows | length > 0 %}
```
Skip the insert entirely if there were zero tests this run (e.g. a
`dbt run` with no `dbt test` step) — an `INSERT ... VALUES` with no rows
is invalid SQL.

```jinja
      {% set query %}
        insert into governance.dbt_test_results
          (invocation_id, run_started_at, test_name, model_name, status,
           severity, failures, execution_time, message)
        values
          {{ test_rows | join(',\n') }}
      {% endset %}
```
Builds one multi-row `INSERT`, joining every tuple with a comma —
standard SQL multi-row insert syntax.

```jinja
      {% do run_query(query) %}
```
Actually executes the SQL against Snowflake. `run_query` is a dbt
built-in for exactly this — running arbitrary SQL from inside a macro.

```jinja
      {% do log('logged ' ~ (test_rows | length) ~ ' test results to governance.dbt_test_results', info=true) %}
```
Prints a one-line confirmation to the console/log so you can see it ran
(e.g. `logged 42 test results to governance.dbt_test_results`).

```jinja
    {% endif %}
  {% endif %}
{% endmacro %}
```
Close everything out.

---

#### 3. `dbt_project.yml` wiring — what each new line does

```yaml
on-run-end:
  - "{{ run_silver_reconciliation_checks() }}"
  - "{{ log_test_results(results) }}"
```
`on-run-end` is a dbt hook that runs a list of SQL/macro statements after
every node in the invocation finishes — but crucially, **before** the
invocation fully closes, so `results` (built from every node that just
ran) is still available to reference. Order matters here only in that
both need `execute = true`, which they get automatically at this stage —
they don't depend on each other, so order between the two lines doesn't
matter.

```yaml
tests:
  +store_failures: true
  +schema: dbt_test_failures
```
This is dbt's own native feature, not custom code. `+store_failures: true`
applied at the top level (`tests:`) means **every** test in the project
gets this behavior, not just some. When a test fails, instead of only
printing to console, dbt runs a `CREATE TABLE ... AS SELECT` (or similar)
containing the exact rows that failed that test, into schema
`dbt_test_failures`. Table name = the test's unique name (same as
`test_name` in `dbt_test_results`) — so you can join the two: the summary
table tells you a test failed and how many rows, the failures schema has
the *actual* offending rows to inspect.

---

#### 4. How to use it day to day

```sql
-- did anything fail in the last run?
select * from governance.dbt_test_results
where invocation_id = (select max(invocation_id) from governance.dbt_test_results)
  and status in ('fail', 'error');

-- see the actual bad rows for a specific failed test
select * from dbt_test_failures.unique_silver_trade_trade_id;

-- trend: is a specific test getting flakier over time?
select test_name, run_started_at, status, failures
from governance.dbt_test_results
where test_name = 'unique_silver_trade_trade_id'
order by run_started_at desc;
```

No manual step needed once wired in — every `dbt build` / `dbt test`
populates both automatically.

### ADR: Custom test-result logging over Elementary package

**Status:** Accepted

**Context:** Need failed dbt test results stored queryably (not just
console/`run_results.json`). Two options evaluated: (1) the open-source
`elementary` dbt package, (2) a custom macro (`log_test_results()`)
writing into a dedicated `governance.dbt_test_results` table, paired with
dbt's native `store_failures: true`.

**Decision:** Custom macro + native `store_failures`. No external
package added.

**Reasoning:**
- **Evidence stays unified.** Every other observability signal in this
  project — bronze ingestion (`_dq_errors`, `dq_audit_log`), silver
  reconciliation (`dq_audit_log`) — already lands in `governance`. A
  custom table keeps test results in the same place, queryable with the
  same joins, following the same `check_type`/`severity` shape documented
  in `07_governance.md`. Elementary would add its own separate schema
  (`elementary` by default) — a second, disconnected evidence store.
- **No current need for what Elementary is actually for.** Elementary's
  real value is alerting (Slack/email/Teams), anomaly detection, and a
  hosted dashboard. Current requirement is "queryable in a table" only —
  none of Elementary's differentiators apply yet, so its cost (extra
  dependency, extra schema, less control over row shape) isn't offset by
  a benefit we'd use.
- **Full control, same pattern as the rest of the layer.** The custom
  table's columns, `on-run-end` timing, and failure-storage behavior
  (`store_failures`) are all native dbt features already used elsewhere
  in this project (ADR-004 in `04_silver.md` — same "standard dbt
  pattern, not project-specific" preference). No dependency on an
  external package's release cycle or breaking changes.

**Revisit if:** the team later needs real alerting, anomaly detection, or
a dashboard — at that point Elementary's actual value proposition kicks
in and the trade-off flips. Until then, adding it would be an unused
dependency, not a capability gap.
## Gold Layer
### Unknown member rows — fact FK columns never NULL

Every dim gets one Unknown member row, `<dim>_sk = -1`, generated via
`union all` inside the dim's own model (not a seed, not a post-hook —
keeps each dim self-contained, one file, same pattern as
`dedup_latest`/`surrogate_key` usage elsewhere in gold).

- **Surrogate key:** `-1`, uniform across all dims (including smart-key
  dims `dim_date`/`dim_time`, which never produce a real `-1` value from
  source data, so no collision risk).
- **Fact-side resolution:** every resolved FK on every fact wraps in
  `coalesce(<dim>.<dim>_sk, -1)` at the same join point — no separate
  cleanup pass. `fact_holding`/`fact_trade_history` inherit
  `account_sk`/`security_sk` pre-coalesced from `fact_trade` (ADR-005/
  ADR-010 lookup), so only their own directly-resolved FKs
  (`status_type_sk`, `status_date_sk`, `status_time_sk`) need a fresh
  coalesce.
- **Reason:** NULL in a join key behaves inconsistently across BI
  tools — some drop the row silently, some show blank, filters behave
  unpredictably. `-1` is a real, joinable, always-present row.
- **Accepted trade-off:** coalescing to `-1` cannot distinguish "FK
  genuinely unresolvable" from "time-range join miss on an otherwise
  valid account/customer" (`fact_trade`, `fact_cashtransaction`,
  `fact_watchitem` all use time-aware joins). Both collapse to the same
  Unknown row. Not fixed at the schema level — revisit only if this
  ever shows up as a real reporting ambiguity.

### Dimension attribute NULL fill

Categorical/code-like dimension attributes (status, tier, gender,
country, job_code, industry_name, etc.) coalesce NULL to the string
`'Unknown'` at model build time, not left blank.

- **Not applied to:** free-text/identifier columns — names, address
  lines, tax_id, DOB, phone numbers, emails. These aren't a bucketable
  category; NULL there stays NULL.
- **Reason:** same as the FK case — a BI tool's "group by tier" should
  show a real `Unknown` bucket, not silently merge NULLs into one
  bucket or drop them depending on tool defaults.


###

- FK integrity: `relationships` tests, fact→dim, severity warn (late-arriving dim tolerance)
-  Surrogate key: `unique` + `not_null` on every dim/fact PK, severity error
- No NULL FK on fact tables 
- Grain test: `unique_combination_of_columns` per fact (e.g. fact_trade_history: trade_id+status_date_sk+status_time_sk+status_type_sk)