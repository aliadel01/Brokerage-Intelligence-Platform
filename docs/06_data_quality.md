# 6. Data Quality, Trust & Consistency Framework

**Scope:** every layer of the pipeline — bronze ingestion, silver
modeling, gold presentation. This document is the DQ reference: what can
go wrong at each layer, which classic **dimension of data quality** each
problem maps to, the concrete control that catches it, and where the
evidence of that control lives.

Data quality is not one checkbox. Each control below is framed against a
named DQ dimension (the standard vocabulary — DAMA-DMBOK / Six Sigma
data-quality literature) so the framework reads as a system, not a list
of unrelated fixes:

| Dimension | Question it answers |
|---|---|
| **Validity** | Is the value the right *shape* and *type*? |
| **Accuracy** | Does the value reflect reality / an independent source? |
| **Completeness** | Is anything missing that should be present? |
| **Consistency** | Does the same fact agree across tables/systems? |
| **Uniqueness** | Is each real-world entity/event represented exactly once? |
| **Timeliness** | Is the data fresh enough to be useful? |
| **Integrity** | Do relationships between tables (keys, FKs) hold? |

## Table of contents
- [6. Data Quality, Trust \& Consistency Framework](#6-data-quality-trust--consistency-framework)
  - [Table of contents](#table-of-contents)
  - [6.1 Bronze Layer — Ingestion Controls](#61-bronze-layer--ingestion-controls)
    - [6.1.1 Validity — schema drift (wrong shape)](#611-validity--schema-drift-wrong-shape)
    - [6.1.2 Validity — bad values (right shape, wrong content)](#612-validity--bad-values-right-shape-wrong-content)
    - [6.1.3 Completeness — silent failures (superseded design)](#613-completeness--silent-failures-superseded-design)
    - [6.1.4 Integrity — the one control value that cannot fail silently](#614-integrity--the-one-control-value-that-cannot-fail-silently)
    - [6.1.5 Consistency — loader output vs. table shape drift](#615-consistency--loader-output-vs-table-shape-drift)
    - [6.1.6 Uniqueness — running the same batch twice by accident](#616-uniqueness--running-the-same-batch-twice-by-accident)
    - [6.1.7 Completeness — a missing control file breaking the safety net silently](#617-completeness--a-missing-control-file-breaking-the-safety-net-silently)
    - [6.1.8 Accuracy — trusting our own pipeline's numbers](#618-accuracy--trusting-our-own-pipelines-numbers)
    - [6.1.9 DQ-as-Control on ingestion](#619-dq-as-control-on-ingestion)
    - [6.1.10 Timeliness — data freshness](#6110-timeliness--data-freshness)
    - [6.1.11 Metadata backbone](#6111-metadata-backbone)
  - [6.2 Silver Layer — Testing \& Validation Framework](#62-silver-layer--testing--validation-framework)
    - [6.2.1 dbt generic tests \& referential integrity](#621-dbt-generic-tests--referential-integrity)
    - [6.2.2 SCD2-specific checks (Account / Customer)](#622-scd2-specific-checks-account--customer)
    - [6.2.3 Business-rule tests (custom singular tests)](#623-business-rule-tests-custom-singular-tests)
    - [6.2.4 Accepted-value domains](#624-accepted-value-domains)
  - [6.3 Reconciliation Framework](#63-reconciliation-framework)
  - [6.4 Test Result Observability \& Logging](#64-test-result-observability--logging)
    - [6.4.1 `governance.dbt_test_results` (summary table)](#641-governancedbt_test_results-summary-table)
    - [6.4.2 `dbt_project.yml` — native `store_failures`](#642-dbt_projectyml--native-store_failures)
    - [6.4.3 `log_test_results()` macro (`macros/log_test_results.sql`)](#643-log_test_results-macro-macroslog_test_resultssql)
    - [6.4.4 Day-to-day usage](#644-day-to-day-usage)
    - [6.4.5 ADR — custom test-result logging over the Elementary package](#645-adr--custom-test-result-logging-over-the-elementary-package)
  - [6.5 Gold Layer — Presentation-Layer Controls](#65-gold-layer--presentation-layer-controls)
    - [6.5.1 Completeness — unknown-member rows, fact FK columns never `NULL`](#651-completeness--unknown-member-rows-fact-fk-columns-never-null)
    - [6.5.2 Consistency — dimension attribute NULL fill](#652-consistency--dimension-attribute-null-fill)
    - [6.5.3 Integrity \& grain tests](#653-integrity--grain-tests)
  - [Open items](#open-items)

---

## 6.1 Bronze Layer — Ingestion Controls

Bronze covers three things before any row is trusted downstream:
**consistency** (every file's shape is verified against expectation —
schema drift and column-list mismatches fail loud, never silently
misaligned), **accuracy/trust** (we don't assume a batch loaded
correctly — idempotency checks, a required control file, and
reconciliation against vendor audit counts all independently confirm
the numbers), and **validity** (every value that fails to cast is kept,
not dropped — `NULL` plus a structured reason in `_dq_errors`, so
nothing disappears without a trace). Silver builds on this foundation.

### 6.1.1 Validity — schema drift (wrong shape)

**Problem:** some sources change shape between batches — e.g.
`Trade.txt`, `HoldingHistory.txt`, `WatchHistory.txt`,
`DailyMarket.txt` carry fewer columns in Batch1 (no
`CDC_FLAG`/`CDC_DSN`) than in Batch2/3.

**Control:** `_split_cdc()` counts actual fields per line rather than
hardcoding "Batch1 = no CDC columns":
- field count == base columns → no CDC columns, backfill `('I', 0)`
- field count == base columns + 2 → CDC columns present, use them
- anything else → `ValueError`, stop loading that file

Same principle for FINWIRE: an unrecognized `RecType` raises
immediately.

*Design rationale:* detecting shape from the data (not the batch
number) keeps the control correct even if a schema shift lands on a
different batch than expected — we're reading the real structure of
the line, not assuming it. Stopping instead of continuing matters
because a wrong shape means the code misunderstood the file — continuing
would misalign every subsequent column. Silently wrong data is worse
than a stopped file.

**Code:** `_split_cdc()` in `delimited_loader.py`; unknown-`RecType`
branch in `load_finwire_source()`, `finwire_loader.py`.

### 6.1.2 Validity — bad values (right shape, wrong content)

**Problem:** a field is present, in the right position, but its
content is invalid — e.g. a date literal `"122006"` that cannot be
parsed.

**Control:** `_safe_cast(raw, caster, col_name)` never raises. Empty
string → `None`. A failed cast → `None` for the value, **plus** a
structured error object: `{column, raw_value, error_type, error_msg}`.
The column lands `NULL`, but *why* is always known — nothing is lost
silently.

*Design rationale:* bad values are normal, expected dirty data.
Stopping an entire batch for one bad date is too strict; fix what can
be fixed and keep moving. Contrast with [6.1.4](#114-integrity--the-one-control-value-that-cannot-fail-silently),
the deliberate exception.

**Code:** `_safe_cast()` in `common.py`, used by every loader.

### 6.1.3 Completeness — silent failures (superseded design)

**Problem (historical):** the original `_safe_cast` returned `None` on
failure with zero trace of what went wrong or where. Bad data
disappeared with no evidence — a completeness failure in the *evidence
trail*, not just the data.

**Control:** `_safe_cast` now returns `(value, error_info)`. Every
failed cast is captured, then all per-row errors are packed by
`_pack_dq_errors()` into a single JSON array in one column,
`_dq_errors`:
- Clean row → `_dq_errors` is `NULL`.
- Dirty row → `_dq_errors` is an inspectable list.

Every bronze table carries this column, applied consistently across
all four loaders (delimited, FINWIRE, XML, audit). The row still lands
in bronze either way — bronze never rejects or quarantines. Deciding
what to *do* about a dirty row (fix, flag, reject) is a silver
decision; bronze's job is load everything, keep the evidence.

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

**Code:** `_safe_cast()` and `_pack_dq_errors()` in `common.py`.

### 6.1.4 Integrity — the one control value that cannot fail silently

**Problem:** `BatchDate.txt`'s date drives the entire batch-idempotency
system ([6.1.6](#116-uniqueness--running-the-same-batch-twice-by-accident)).
If this single value silently became `NULL`, every future run for that
batch would be corrupted with no warning.

**Control:** `asofdate` still goes through `_safe_cast` for a
structured error, but `load_batch_date` **raises** on cast failure
instead of soft-failing — stopping the batch immediately. This is the
one deliberate exception to [6.1.2](#112-validity--bad-values-right-shape-wrong-content)'s
"bad value = NULL and continue" rule. Because a row only ever lands in
`bronze_batch_control` when `asofdate` is valid, the table is clean by
construction — it doesn't even carry a `_dq_errors` column.

**Code:** `load_batch_date()` in `audit_loader.py`.

### 6.1.5 Consistency — loader output vs. table shape drift

**Problem:** if a loader's column list and the target table's actual
columns aren't kept explicitly in sync, they can silently drift (e.g.
a column added to one but not the other), and `COPY INTO` writes into
the wrong columns.

**Control:** `copy_into()` always takes an **explicit column list**,
never positional-only. `ON_ERROR = 'ABORT_STATEMENT'` — if a
genuinely bad-typed row somehow reaches Snowflake, the whole file's
load aborts loudly instead of partially loading.

**Code:** `copy_into()` in `snowflake_client.py`.

### 6.1.6 Uniqueness — running the same batch twice by accident

**Problem:** without a check, re-running a batch (on purpose or by
mistake) duplicates every row in bronze.

**Control:**
- Before loading, check `bronze_batch_control` for an existing row
  with this `_batch_id`:
  - No existing row → proceed normally.
  - Existing row, no `--force` → stop immediately (`RuntimeError`) —
    loud on purpose, an accidental re-run must never silently
    duplicate data.
  - Existing row, `--force` → wipe every row for that `_batch_id`
    across all bronze tables in one transaction (a failed wipe rolls
    back cleanly), then load fresh.
- `bronze_batch_control` loads **first**, before any other source — so
  an interrupted run is correctly detected as "already attempted" on
  the next run, instead of silently reloading everything.

**Code:** `run_batch()` and `force_delete_batch()` in `main.py`.

> [!NOTE]
> Silver already protects against duplicate data via
> `dedup_latest`/`_row_hash` ([6.2](#62-silver-layer--testing--validation-framework)),
> so a stray re-ingested batch wouldn't corrupt downstream results
> either way. This bronze-layer guard exists for a different reason:
> preventing storage bloat from repeated full/partial batch loads
> sitting in bronze indefinitely — not a second line of defense
> against bad data reaching silver.

### 6.1.7 Completeness — a missing control file breaking the safety net silently

**Problem:** if `BatchDate.txt` is missing and simply skipped (as every
other optional source is), the batch finishes without ever writing a
row to `bronze_batch_control`. The next run then sees "no existing
row," assumes nothing was ever loaded, and reloads everything on top —
silent duplication, no warning.

**Control:** `BatchDate.txt` is the **one required file**. Every other
source is skipped silently if missing (batch scope is expressed by
which files exist on disk). `BatchDate.txt`'s absence raises
`FileNotFoundError` immediately, before any other source is touched.

**Code:** the `BatchDate.txt` check in `run_batch()`, `main.py`.

### 6.1.8 Accuracy — trusting our own pipeline's numbers

**Problem:** a loader can run "successfully" and still have silently
dropped or duplicated rows somewhere in transit.

**Control:** loaded row counts are compared against the vendor-supplied
`RowCount` values in `*_audit.csv` for the same batch — an independent
external source, not the pipeline's own count. A mismatch prints a
`WARNING`; it does **not** stop the batch, since a mismatch can have a
legitimate explanation. This is a "someone should notice and
investigate" signal, not an automatic failure. This is the classic DQ
pattern — check your numbers against an outside source, don't just
trust your own process — and it already caught a real off-by-one
during testing.

**Code:** reconciliation query and comparison loop at the end of
`run_batch()`, `main.py`.

### 6.1.9 DQ-as-Control on ingestion

The full evidence-logging mechanics (`governance.dq_audit_log`,
`log_dq_event()`) live in `07_governance.md` §7.9 (Operational
Auditability) — that section is the canonical reference; this doc only
tracks *which check* catches *which* DQ dimension.

### 6.1.10 Timeliness — data freshness

dbt source freshness is standardized on a **24-hour warning threshold**
on `_loaded_at`, applied only to incremental tables (Batch2/3).
Static historical (Batch1-only) tables are explicitly excluded — a
freshness check on a table that only ever loads once would be a
guaranteed, meaningless false positive.

**Example usage:**
```bash
# run a freshness check on the bronze_account source
dbt source freshness --select bronze_account

# result
16:16:36  1 of 10 START freshness of bronze.bronze_account ............................... [RUN]
16:16:36  1 of 10 WARN freshness of bronze.bronze_account ................................ [WARN in 0.95s]
```

### 6.1.11 Metadata backbone

Every bronze row carries `_batch_id`, `_source_file`, `_loaded_at`,
`_row_hash`, `_dq_errors`. Together these give **lineage** (where a row
came from — see `07_governance.md` §7.4), **dedup/QA signal**
(`_row_hash` — has this exact row been seen before), and **traceability**
(`_dq_errors` — exactly what was dirty, and why).

**Code:** `compute_row_hash()` and `_pack_dq_errors()` in `common.py`.

---

## 6.2 Silver Layer — Testing & Validation Framework

Full modeling logic is documented in `04_silver.md`; this section is
the DQ-control summary layered on top of it.

- **Uniqueness / Integrity:** deduplication standardized via one
  `dedup_latest` macro, two strategies — **state-tracking** (latest
  entity state, e.g. `silver_trade`) and **append-only** (exact-dupe
  removal only, distinct version events retained, e.g.
  `silver_account`, `silver_customer`). Resolves dual-source overlap so
  no entity is double-counted.
- **Completeness:** partial CDC payloads handled via
  `LAST_VALUE(...) IGNORE NULLS` forward-fill, executed across the full
  chronological event sequence *before* the daily collapse step — this
  ordering prevents historical attributes from reverting to `NULL`
  during aggregation.
- **Consistency:** heterogeneous source vocabularies (flat-file CDC +
  XML) unified into standardized domains — e.g. `status_id` →
  `ACTV`/`INAC`, `cdc_flag` → `I`/`U`. Non-entity noise events filtered
  out to keep downstream structures clean.
- **Integrity (fan-trap prevention):** current state decoupled from
  lifecycle transitions (ADR-002) — `silver_trade` (latest state) split
  from `silver_trade_history` (transition history). This structurally
  prevents a Kimball fan trap in gold — no false multiplication of
  financial measures (e.g. `SUM(commission)`) during aggregation.
- **Determinism / lineage:** deterministic processing via an explicit
  composite ordering key (`_batch_id, action_ts, _cdc_dsn, _loaded_at`)
  to break ties and guarantee idempotent execution. Explicit lineage
  via `_source_model`, rather than an implicit proxy like
  `_batch_id = 1`.

### 6.2.1 dbt generic tests & referential integrity

Core dbt tests across silver: `not_null`, `unique`,
`unique_combination_of_columns`, `accepted_values` for schema/metadata
integrity. `relationships` tests on key dependencies (`silver_trade`,
`silver_account`, `silver_holding_history`, `silver_trade_history`),
severity set to **warn** rather than **error**, so orphan records are
logged, not build-breaking.

- Every silver model's primary key — business key, surrogate key,
  `_row_hash`, or composite key — is enforced via `unique` + `not_null`.
- **Severity: error** → `not_null`, `unique`, `unique_combination_of_columns`.
- **Severity: warn** → `accepted_values`, `relationships` (orphan /
  late-arriving-dimension tolerance).

### 6.2.2 SCD2-specific checks (Account / Customer)

Purpose-built assertions validate the SCD2 modeling contract, each
targeting a distinct integrity dimension:

| Test | DQ dimension | What it asserts |
|---|---|---|
| `assert_scd2_active_flag_integrity` | Uniqueness | Each entity key (`account_id`/`customer_id`) resolves to exactly one `is_current = true` row |
| `assert_no_overlapping_date_ranges` | Integrity | No `valid_from_date` overlaps the preceding version's `valid_to_date` |
| `assert_scd2_date_continuity` | Consistency | Strict date-chain continuity: `valid_to_date(N) = valid_from_date(N+1) - 1 day`, zero gaps |
| `assert_forward_fill_sanity` | Completeness | Tracked business columns never regress to `NULL` across versions once populated |

### 6.2.3 Business-rule tests (custom singular tests)

- **Illegal trade status state transitions** — a singular test asserts
  the trade lifecycle never moves through an invalid status sequence
  `test/silver_trade_history/assert_trade_status_order.sql`.
- **False versioning in tracked SCD2 entities** — a singular test
  asserts a new SCD2 version is only created when a *tracked* column
  actually changed value, not on every event (Uniqueness — prevents
  version-count inflation).


### 6.2.4 Accepted-value domains

`accepted_values` enforced on status/type code columns:

| Column | Domain |
|---|---|
| `silver_trade_type.trade_type_id` | `TMB`, `TMS`, `TSL`, `TLS`, `TLB` |
| `silver_status_type.status_id` | `ACTV`, `CMPT`, `CNCL`, `PNDG`, `SBMT`, `INAC` |

---

## 6.3 Reconciliation Framework

**Dimension: Accuracy / Completeness**, applied structurally across
layer boundaries — not just at ingestion.

**Problem:** dedup, collapse, and filter logic inside silver models
(`dedup_latest`, SCD2 day-collapse, `where t_id is not null`, etc.) can
silently drop more or fewer rows than intended. Gold has the same
exposure one layer up — FK-resolution joins, `dedup_latest` collapses
(`dim_company`, `dim_security`), and inner-join lookups
(`fact_holding`, `fact_trade_history`) can drop or multiply rows in
ways no relationship or grain test catches.

**Control:** one shared macro, `log_reconciliation()`
(`macros/reconciliation/log_reconciliation.sql`), holds the delta-percent math,
severity assignment, and the insert into `governance.dq_audit_log` in
exactly one place. Two thin layer-specific wrappers build the
comparison and call it:

- `log_silver_reconciliation()` — bronze vs. silver, **per `_batch_id`**.
- `log_gold_reconciliation()` — silver vs. gold, **total row count**,
  not per-batch. Gold recomputes current state in full every run, and
  most gold tables (every dim) don't carry a `_batch_id` at all, so a
  per-batch comparison doesn't apply the way it does bronze→silver.
  One row is logged per model, `batch_id = -1` (a sentinel — same `-1`
  convention as every dim's Unknown-member row). The synthetic Unknown
  row is excluded from gold's count before comparing, controlled by a
  `has_unknown_row` flag per call (`true` for dims, `false` for facts)
  — otherwise every dim would show a false "+1" delta.

- `check_type = 'silver_reconciliation'` / `'gold_reconciliation'`
- `severity = 'PASS'` if delta within threshold, else `'WARNING'` —
  **never fatal**, same reasoning as bronze reconciliation: a mismatch
  may be expected behavior (dedup/collapse/filtered join), not a bug —
  a human reviews it rather than the build failing automatically.
- Threshold is **per model**, not global — expected delta varies
  hugely by archetype.

**Silver (bronze vs. silver):**

| Model type | Example | Threshold | Why |
|---|---|---|---|
| Archetype A pass-through | `silver_hr` | 1% | Should be near-exact 1:1 |
| Quasi-CDC append-only | `silver_holding_history`, etc. | 2% | Only exact-dupe rows drop |
| Prospect (SCD1 snapshot) | `silver_prospect` | 10% | Intra-batch dupes collapse |
| Trade history (dual union, Batch1 excluded from one side) | `silver_trade_history` | 30% | Structural, filtered union |
| Account / Customer (dual-source, day-collapse, tracked-col filter) | `silver_account`, `silver_customer` | 50% | Heavy collapse by design |
| Trade (many events → 1 row per `trade_id`) | `silver_trade` | 80% | Collapse is the entire point of the model |

**Gold (silver vs. gold):**

| Model type | Example | Threshold | Why |
|---|---|---|---|
| Dim: direct pass-through / SCD2-versioned, + Unknown row | `dim_date`, `dim_time`, `dim_statustype`, `dim_tradetype`, `dim_broker`, `dim_customer`, `dim_account`, `dim_prospect` | 2% | Near-exact 1:1, plus one synthetic row |
| Dim: `dedup_latest` collapse, + Unknown row | `dim_company`, `dim_security` | 60% | Many silver versions → one gold row per key, by design |
| Fact: direct pass-through, left-join FK resolution only | `fact_trade`, `fact_cashtransaction`, `fact_watchitem`, `fact_market_history`, `fact_company_financials` | 2% | No row-dropping join |
| Fact: inner join to `fact_trade` for derived FKs | `fact_holding`, `fact_trade_history` | 15% | Orphan rows (unmatched `trade_id`) can drop |

> [!NOTE]
> These thresholds are starting defaults, not confirmed business
> requirements — no official "acceptable delta %" has been specified
> yet. Revisit once real batch volumes are known; a fixed percentage
> can hide a real problem at high volume, or false-alarm at low volume.
> Every raw count stays queryable in `dq_audit_log` regardless of
> threshold, so nothing is lost even if a threshold is later found
> wrong.

`governance.dq_audit_log` already exists (created by the ingestion-layer
`log_dq_event()` — `07_governance.md` §7.9); no new DDL is needed for
either check.


**Code:** `macros/log_reconciliation.sql` (shared core),
`macros/log_silver_reconciliation.sql` /
`macros/log_gold_reconciliation.sql` (layer-specific comparison
builders), wired
via `on-run-end` in `dbt_project.yml`.


**Example usage:**
```sql
SELECT * FROM BROKERAGE_DWH.GOVERNANCE.DQ_AUDIT_LOG
```

| LOG_ID | BATCH_ID | CHECK_TYPE | SOURCE_FILE | EXPECTED_VALUE | ACTUAL_VALUE | SEVERITY | MESSAGE | LOGGED_AT |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `1102` | `1` | `silver_reconciliation` | `silver_trade` | 390978 | 390978 | **PASS** | expected=390978 actual=390978 delta=0.00% (threshold 80%) | 2026-08-16 ... |
| `1041` | `1` | `silver_reconciliation` | `silver_customer` | 14572 | 5702 | **WARNING** | expected=14572 actual=5702 delta=60.87% (threshold 50%) | 2026-08-16 ... |
| `1101` | `1` | `silver_reconciliation` | `silver_account` | 12860 | 12239 | **PASS** | expected=12860 actual=12239 delta=4.83% (threshold 50%) | 2026-08-16 ... |


---

## 6.4 Test Result Observability & Logging

**Dimension: Operational auditability of the DQ framework itself** —
who tested what, when, with what result, queryable after the fact
rather than only visible in console output at run time.

### 6.4.1 `governance.dbt_test_results` (summary table)

| Column | Purpose |
|---|---|
| `invocation_id` | Groups every test from a single `dbt build`/`dbt test` run |
| `run_started_at` | Run timestamp |
| `test_name` | dbt's generated unique test name |
| `model_name` | The model the test targets |
| `status` | `pass` / `fail` / `error` |
| `severity` | `error` / `warn`, from the test's own config |
| `failures` | Row count that failed (nullable — a test can error before counting) |
| `execution_time` | Test runtime |
| `message` | dbt's own failure message text |

### 6.4.2 `dbt_project.yml` — native `store_failures`

```yaml
tests:
  +store_failures: true
  +schema: dbt_test_failures
```

Native dbt feature, not custom code. Applied at the top level (`tests:`)
so **every** test in the project gets this behavior. On failure, dbt
persists the exact offending rows into schema `dbt_test_failures`,
table name = the test's unique name (same as `test_name` in
`dbt_test_results`) — the summary table tells you a test failed and how
many rows; the failures schema has the actual bad rows to inspect.

### 6.4.3 `log_test_results()` macro (`macros/log_test_results.sql`)

Runs from `on-run-end`, where dbt hands every macro a built-in
`results` list — one `Result` object per node that ran in the
invocation (models, tests, seeds). The macro filters to `resource_type
== 'test'`, resolves each test's parent model (`attached_node` for
generic tests, `depends_on.nodes[0]` for singular tests), reads its
configured `severity` (defaulting to dbt's own `error` default), and
batches every row into one multi-row `INSERT` rather than one insert
per test — cheaper and avoids partial-write races.

### 6.4.4 Day-to-day usage

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

No manual step needed once wired in — every `dbt build`/`dbt test`
populates both tables automatically.

### 6.4.5 ADR — custom test-result logging over the Elementary package

**Context:** need failed-test results stored queryably, not only in
console output / `run_results.json`. Two options evaluated: (1) the
open-source `elementary` dbt package, (2) a custom macro
(`log_test_results()`) writing into `governance.dbt_test_results`,
paired with dbt's native `store_failures: true`.

**Decision:** custom macro + native `store_failures`. No external
package added.

**Reasoning:**
- **Evidence stays unified.** Every other observability signal in this
  project — bronze `_dq_errors`/`dq_audit_log`, silver/gold
  reconciliation — already lands in `governance`. A custom table keeps
  test results in the same place, queryable with the same joins,
  following the same `check_type`/`severity` shape as the rest of the
  framework. Elementary would add its own separate schema (`elementary`
  by default) — a second, disconnected evidence store.
- **No current need for what Elementary is actually for.** Elementary's
  real value is alerting (Slack/email/Teams), anomaly detection, and a
  hosted dashboard. The current requirement is "queryable in a table"
  only — none of Elementary's differentiators apply yet, so its cost
  (extra dependency, extra schema, less control over row shape) isn't
  offset by a benefit that gets used.
- **Full control, consistent pattern.** Same native-dbt-feature
  preference used elsewhere in this project (`04_silver.md` ADR-004).
  No dependency on an external package's release cycle or breaking
  changes.

**Revisit if:** the team later needs real alerting, anomaly detection,
or a dashboard — at that point Elementary's value proposition applies.
Until then, adding it would be an unused dependency, not a capability
gap.

---

## 6.5 Gold Layer — Presentation-Layer Controls

### 6.5.1 Completeness — unknown-member rows, fact FK columns never `NULL`

Every dimension gets one Unknown member row, `<dim>_sk = -1`, generated
via `union all` inside the dim's own model — not a seed, not a
post-hook, keeping each dim self-contained (same pattern as
`dedup_latest`/`surrogate_key` usage elsewhere in gold).

- **Surrogate key:** `-1`, uniform across all dims (including smart-key
  dims `dim_date`/`dim_time`, which never produce a real `-1` from
  source data — no collision risk).
- **Fact-side resolution:** every resolved FK on every fact is wrapped
  in `coalesce(<dim>.<dim>_sk, -1)` at the join point — no separate
  cleanup pass. `fact_holding`/`fact_trade_history` inherit
  `account_sk`/`security_sk` already-coalesced from `fact_trade`
  (`05_gold.md` ADR-005/ADR-010 lookup); only their own directly
  resolved FKs (`status_type_sk`, `status_date_sk`, `status_time_sk`)
  need a fresh coalesce.
- **Reason:** `NULL` in a join key behaves inconsistently across BI
  tools — some drop the row silently, some show blank, filters behave
  unpredictably. `-1` is a real, joinable, always-present row.
- **Accepted trade-off:** coalescing to `-1` cannot distinguish "FK
  genuinely unresolvable" from "time-range join miss on an otherwise
  valid account/customer" (`fact_trade`, `fact_cashtransaction`,
  `fact_watchitem` all use time-aware joins). Both collapse to the same
  Unknown row. Not fixed at the schema level — revisit only if this
  shows up as a real reporting ambiguity.

### 6.5.2 Consistency — dimension attribute NULL fill

Categorical/code-like dimension attributes (status, tier, gender,
country, job_code, industry_name, etc.) coalesce `NULL` to the literal
string `'Unknown'` at model build time, rather than left blank.

- **Not applied to** free-text/identifier columns — names, address
  lines, tax ID, DOB, phone numbers, emails. These aren't a bucketable
  category; `NULL` stays `NULL`.
- **Reason:** same logic as the FK case — a BI tool's "group by tier"
  should show a real `Unknown` bucket, not silently merge NULLs into
  one bucket or drop them depending on tool defaults.

### 6.5.3 Integrity & grain tests

- **FK integrity:** `relationships` tests, fact → dim, severity
  **warn** (tolerates late-arriving dimensions).
- **Surrogate key integrity:** `unique` + `not_null` on every dim/fact
  PK, severity **error**.
- **No NULL FK on fact tables** — enforced by the coalesce-to-`-1`
  pattern above, not by a separate test.
- **Grain test:** `unique_combination_of_columns` per fact, e.g.
  `fact_trade_history`: `trade_id + status_date_sk + status_time_sk +
  status_type_sk`.

---

## Open items

1. Reconciliation thresholds ([6.3](#63-reconciliation-framework)) are
   working defaults, not confirmed business SLAs.
2. No alerting/anomaly-detection layer on top of
   `governance.dbt_test_results` yet — deliberate, see the Elementary
   ADR ([6.4.5](#145-adr--custom-test-result-logging-over-the-elementary-package)).
3. Freshness threshold (24h) is a single global default for all
   incremental sources — not yet tuned per-source SLA.

