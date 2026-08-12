# Governance, Compliance and Ownership

This document outlines the governance, compliance, and ownership practices for the data platform.

## Table of Contents
  1. [Roles & Access Control](#roles--access-control)
  2. [Ownership](#ownership)
  3. [Data Classification](#data-classification)
  4. [PII Masking Policies](#pii-masking-policies)
  5. [DQ-as-Control on Ingestion](#dq-as-control-on-ingestion)
  6. [Retention & Erasure](#retention--erasure)
  7. [Historical Reconstruction and Access Auditing](#historical-reconstruction-and-access-auditing)
  8. [Regulatory Mapping](#regulatory-mapping)

## Roles & Access Control

Five roles, split along two axes: **service account vs. human login**, and
**what layer they can see**. See `sql/roles.sql` for the full grant script.

1. **`role_bronze_loader`** — **service account**, no human login. **Owns bronze
   only**: `SELECT`/`INSERT`/`DELETE` on every bronze table, plus
   `READ`/`WRITE` on the ingestion stage. This is the identity the Python
   ingestion pipeline (`main.py`) authenticates as — never a human
   credential. Also has `INSERT`/`SELECT` on `governance.dq_audit_log` to
   log reconciliation mismatches.

2. **`role_custodian`** — **human login**, **read-only** across all three layers
   ***(bronze, silver, gold)***, plus read access to `governance`. This is the
   "sees everything unmasked" role: the masking policies below explicitly
   check for this role (or `ACCOUNTADMIN`) to return real PII values. In
   practice, this is the project owner/maintainer — the one identity
   trusted to see raw data end-to-end for debugging, audits, and
   verification.

3. **`role_analyst`** — **human login**, **read-only** across ***silver and gold***
   only. No bronze access at all (enforced by explicit `REVOKE`, not just
   omission). `restricted_pii` columns appear masked (`***MASKED***` /
   `NULL`) to this role, since it falls outside the unmask condition in
   the masking policies. `confidential` columns (including the prospect
   financial attributes above) are **not** masked and are visible to this
   role. Represents any downstream consumer who needs the modeled data
   but has no business reason to see raw bronze or unmasked identity PII.

4. **`role_dbt_prod_ci`** — **service account** for dbt runs, no human login.
   **Read-only on bronze** (dbt selects from it, never writes to it), full
   build rights (`CREATE TABLE`/`CREATE VIEW`/`ALL`) on silver and gold —
   this is the role that actually materializes the dbt models. Also logs
   to `governance.dq_audit_log` (e.g. failed dbt tests). Kept deliberately
   separate from `role_bronze_loader` — Segregation of Duties: one
   identity able to both write raw bronze *and* rebuild every downstream
   transformation would mean a single compromised credential (or a single
   bug) could corrupt the entire pipeline end-to-end, with no boundary in
   between.

5. **`role_steward`** — **human login**, **read-only** on silver and gold
   (no bronze). Distinct from `role_custodian`: the steward owns the
   *business meaning* of a model (definitions, classification correctness,
   whether a metric still matches what the business expects) — not raw,
   unmasked debugging access across all three layers. `restricted_pii`
   stays masked to this role, same as `role_analyst`. This is the identity
   recorded as `data_steward` in model `meta` (see Ownership below).

## Ownership

Every silver and gold model carries three `meta` fields, set once as a
project-wide default in `dbt_project.yml` (`+meta` under `models.silver`
and `models.gold`) rather than repeated per model in the `.yml` schema
files — dbt merges `meta` down through folder-level config, so one
project-level block covers every model in that folder, including
subfolders (`archetype_a`, `fact_tables`, etc.), and a specific model can
still override any single field in its own schema.yml if it ever needs a
different owner. `sources.yml` has no equivalent inheritance mechanism —
`bronze` is a single source, so its `meta` is set once directly on the
source.

| Field | Meaning | Current value |
|---|---|---|
| `owner` | The project/team accountable for the layer overall. | `brokerage-data-platform` |
| `data_steward` | Owns business meaning — definitions, classification correctness, whether logic still matches what the business expects. | `role_steward` |
| `technical_owner` | Owns the pipeline/code — who gets paged when a load or a model build breaks. | `role_custodian` |

Right now `role_steward` and `role_custodian` are the same person playing
both roles on a one-person project — recorded as two distinct roles
regardless, so the split is already in place (grants, docs, `meta`
values) the moment a second person joins and only one of the two hats
needs handing off.

## Data Classification

Classification happens where the value first lands, not where it's first
"understood." Bronze carries the same raw PII/financial values silver
does — a customer's name is just as sensitive as raw CDC as it is after
SCD2 modeling — so classification starts at ingestion, not at silver.
What's deferred to silver is *business meaning* (current status, deduped
state), not *sensitivity*.

Four levels are used, in ascending sensitivity:

| Level | Meaning | Example |
|---|---|---|
| `public` | No restriction | calendar dates, industry codes |
| `internal` | Employees only, no external sharing | batch IDs, tax rate codes |
| `confidential` | Business-sensitive, SOX-relevant | trade prices, account IDs, company financials |
| `restricted_pii` | Personal data, GDPR-relevant | customer name, tax ID, address, phone, email, DOB |

Also we added a description to each model in the `_silver__models.yml` and `_gold__models.yml` files, so that when you run `dbt docs generate`, the documentation will show the classification of each column next to its definition.

### Enforcement — the Snowflake tag is the source of truth

```sql
CREATE TAG IF NOT EXISTS data_classification
    ALLOWED_VALUES 'public', 'internal', 'confidential', 'restricted_pii';
```

Every PII/financial column across all tables — bronze, silver, and gold —
is tagged directly at the warehouse level via
`ALTER TABLE ... MODIFY COLUMN ... SET TAG`. This is deliberate: the tag
is queryable and enforceable regardless of which tool (dbt, a raw SQL
client, a BI tool) touches the column — it doesn't depend on anyone
reading documentation first.

See `sql/classification_tags.sql` for the full tagging script, covering
bronze (archetypes A–E), silver, and gold.

```sql
SELECT OBJECT_NAME, COLUMN_NAME, TAG_VALUE
FROM TABLE(
    brokerage_dwh.information_schema.TAG_REFERENCES_ALL_COLUMNS(
        'brokerage_dwh.bronze.bronze_customer', 'table'
    )
) LIMIT 5;
```
| OBJECT_NAME | COLUMN_NAME | TAG_VALUE |
| :--- | :--- | :--- |
| BRONZE_CUSTOMER | C_ID | confidential |
| BRONZE_CUSTOMER | C_TIER | confidential |
| BRONZE_CUSTOMER | C_ST_ID | internal |
| BRONZE_CUSTOMER | C_LCL_TX_ID | internal |
| BRONZE_CUSTOMER | C_NAT_TX_ID | internal |


### Restricted PII Access Audit Example 


Snowflake `ACCESS_HISTORY` is used to audit access to columns classified as restricted_pii.

For the `silver_hr` example, the restricted columns are:

`first_name`, `last_name`, `middle_initial`, `phone`

The audit is scoped to the last 30 days and returns the time, user, query ID,
and SQL text for queries that accessed at least one of these columns.
You can find an example query in `sql/restricted_pii_access_history.sql`

This provides an operational audit trail answering:

> Who accessed restricted PII, when did they access it, and which query did they use?
### Documentation — schema.yml for all layers mirrors the tag

`meta.classification` is set on the same columns in
`sources.yml` / `_silver__models.yml` / `_gold__models.yml`, so the
classification is visible directly in `dbt docs generate` output next to
each column's definition — useful for anyone reading the model without
querying Snowflake directly. **The Snowflake tag is authoritative; the
YAML is a mirror of it, not a second source of truth.**



## PII Masking Policies

The control rules that enforce classification policy on `restricted_pii`
columns live in `governance` too (see `sql/masking_policy`), as reusable
policy definitions rather than one-off logic per table:

- **Masking policies** (`mask_pii_string`, `mask_pii_date`,
  `mask_pii_numeric`) — applied per-column via
  `ALTER TABLE ... SET MASKING POLICY` on `restricted_pii` columns across
  bronze, silver, and gold. Enforcement is role-based via
  `CURRENT_ROLE()`: `role_custodian` and `ACCOUNTADMIN` see real values,
  every other role (including `role_analyst`) sees the masked form —
  automatically, on every query, with no per-query logic needed on the
  consumer's side.
- `mask_pii_numeric`, `mask_pii_date` return `NULL` for masked values, since a numeric or date value has no
  obvious "masked" string representation. `mask_pii_string` returns
  `***MASKED***` for masked string values.

**Real Example**:

```sql
SELECT middle_initial FROM brokerage_dwh.silver.silver_hr LIMIT 5;
```

<div style="display: flex; gap: 20px;">

  <div style="flex: 1;">

  ### Masked State using `role_analyst`

  | # | MIDDLE_INITIAL |
  |---|---|
  | 1 | \*\*\*MASKED\*\*\* |
  | 2 | \*\*\*MASKED\*\*\* |
  | 3 | \*\*\*MASKED\*\*\* |
  | 4 | \*\*\*MASKED\*\*\* |
  | 5 | \*\*\*MASKED\*\*\* |
  </div>

  <div style="flex: 1;">

  ### Unmasked State using `role_custodian`

  | # | MIDDLE_INITIAL |
  |---|---|
  | 1 | R |
  | 2 | X |
  | 3 | N |
  | 4 | V |
  | 5 | I |

  </div>

</div>

## Historical Reconstruction and Access Auditing

Governance is not only about defining who can access data or how PII is
masked. It also requires being able to answer two operational questions:

1. What did an account look like at a specific point in time?
2. Who accessed restricted PII?

The following SQL artifacts provide those two controls for the Silver layer.

### Point-in-Time Reconstruction — `silver_account`

The `silver_account` model maintains versioned account records with
`valid_from_date` and `valid_to_date`. This allows the model to reconstruct
the account state that was valid at a specific point in time rather than
returning only the current version.

The query accepts an `as_of_date` and returns the applicable version of each
account for that date.

```sql
SET as_of_date = '2011-06-30';

SELECT
    account_version_sk,
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    cdc_flag,
    valid_from_date,
    valid_to_date,
    is_current,
    _batch_id,
    _source_table
FROM brokerage_dwh.silver.silver_account
WHERE valid_from_date <= $as_of_date
  AND (
      valid_to_date >= $as_of_date
      OR valid_to_date IS NULL
  )
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id
    ORDER BY valid_from_date DESC
) = 1
ORDER BY account_id;
```

## DQ-as-Control on Ingestion
Scope: bronze-layer ingestion (`main.py`).

### `governance.dq_audit_log` (Snowflake table)

Structured, queryable DQ evidence trail. Replaces `print()`-only reconciliation
warnings. Permanent record — not overwritten, not rotated.

### `log_dq_event()` (`main.py`)

Inserts one row per DQ check result. Own transaction: commits on success,
rolls back and raises `RuntimeError` on failure — a broken audit-log insert
must not silently disappear.


  ```python
  def log_dq_event(conn, batch_id, check_type, source_file,
                    expected_value, actual_value, severity, message):
      ...
  ```

## Retention & Erasure  

### Erasure requests
Built `erasure_log` table to track erasure requests and actions. This is a permanent record of what was erased, when, and by whom. The table includes the following columns:

- `erasure_id`: Unique identifier for each erasure request.
- `customer_id`: Identifier for the customer whose data is being erased.
- `requested_at`: Timestamp when the erasure request was made.
- `erased_at`: Timestamp when the data was actually erased.
- `reason`: Reason for the erasure request.
- `status`: Current status of the erasure request.
- `affected_layers`: Layers of the data that are affected by the erasure.
- `requested_by`: Person or system that requested the erasure.
- `notes`: Additional notes about the erasure request.

When an erasure request is processed:
the system will make the PII columns `NULL` in the affected layers and log the action in the `erasure_log` table. We will not delete the rows themselves because this would break referential integrity and historical reconstruction. Instead, we will nullify the PII columns and log the action in the `erasure_log` table.

**Code Example**: we will do that in each layer with a query like the following:
```sql
UPDATE silver_customer
SET
    email = NULL,
    phone = NULL,
    tax_id = NULL,
    ...
WHERE customer_id = 123;
```

### Retention Policy
Raw PII is retained for 90 days to support recovery, debugging, and controlled reprocessing.
This limits unnecessary long-term exposure while providing a reasonable operational recovery window.

**Code Example**:
```sql
ALTER TABLE bronze_customer
SET DATA_RETENTION_TIME_IN_DAYS = 90;
```
### Reconciliation check — full audit artifact

Existing reconciliation query (audit file `RowCount` vs actual bronze rows
loaded) now logs **every** outcome, not just failures:

| Outcome | `check_type` | `severity` |
|---|---|---|
| counts match | `reconciliation_check` | `PASS` |
| counts differ | `reconciliation_mismatch` | `WARNING` |
| audit expects a source, none was loaded | `reconciliation_mismatch` | `WARNING` |

Decision: logging passes too (not only exceptions) means the table shows
reconciliation *coverage* per batch — an auditor can confirm the check ran
for every source, not just see the failures. Mismatches remain non-fatal:
a mismatch may have a legitimate explanation, so someone reviews it rather
than the batch aborting automatically.

### Operational logging (`logging_setup.py`)

Separate from the DQ audit trail — this is process/progress output, not
business evidence. Not written to Snowflake.

```python
def get_logger(batch_id: int, log_file: str | None = None) -> logging.LoggerAdapter:
    ...
```

- Console handler always on; file handler added when `--log-file` is passed.
- `LoggerAdapter` injects `batch_id` into every record automatically.
- All `main.py` `print()` calls replaced with `log.info(...)` / `log.warning(...)`.
- `force_delete_batch()` and `run_batch()` now take a `log` parameter.

### `_dq_errors` — row/column-level cast errors (Problem 3 in `06_data_quality`, pre-existing)
 
Separate again from the two logs above — this is per-row evidence, not
process output and not a batch-level audit table.
We talked about it in `06_data_quality.md`.
 
### Where each piece of evidence lives
 
| | `_dq_errors` | DQ audit trail | Operational log |
|---|---|---|---|
| Where | column on every bronze table | `governance.dq_audit_log` (Snowflake) | stdout / optional file |
| Grain | per row, per column | per batch, per check | per process run |
| What | failed casts (raw value, error type/msg) | check results (pass/fail, expected/actual) | progress, errors, debug |
| Lifetime | permanent, lives with the row | permanent, queryable | transient, run-scoped |
| Who acts on it | silver (fix/flag/reject) | auditor/reviewer | developer |
| Written by | `_safe_cast()` / `_pack_dq_errors()` | `log_dq_event()` | `log.info` / `log.warning` (stdlib `logging`) |
 
Keeping these three apart avoids forcing operational noise (row counts,
"batch complete") into a schema meant for check evidence, avoids losing
DQ evidence in a log stream nobody archives, and keeps row-level cast
errors traveling with the row itself rather than off in a separate table
that would need a join to reconstruct which row was dirty.
 
## Regulatory Mapping
- **SOX-style**: apply to trade/balance data — `fact_trade`, `fact_trade_history`, `silver_trade`. Control need: integrity + audit trail + no double-count risk on financial measure (commission, balance). ADR-002/ADR-009 split (silver+gold) exist for this exact reason — one row latest state, one row full status lineage — stop fan-trap where `SUM(commission)` multiply by transition count. `_row_hash`, `_batch_id`, `_cdc_dsn` ordering give traceable lineage: auditor can reconstruct "what value, when, from what source event." SOX-style control here = financial reporting accuracy + immutable audit trail, not raw storage security.

- **GDPR-style**: apply to customer PII — `silver_customer`/`dim_customer`. Holds name, address, email, DOB, tax_id, phone. SCD2 (ADR-001, day-grain, tracked columns include name/address/email) mean history kept forever by design — direct tension vs GDPR erasure right + data minimization. Carried-only fields (phone, DOB, tax_id) still land in table even though not "tracked" for versioning — still personal data, still in scope. Control need: retention policy + deletion/anonymization procedure on SCD2 history, access control on PII columns, and lawful-basis documentation for why full history kept (business need: identity/location/status audit — spec says so).

- **PCI-DSS**: not apply. No PAN, card number, CVV, expiry field anywhere in dictionary or model — `CashTransaction`/`fact_cashtransaction` carry amount + type only, no payment-instrument detail. Whole pipeline (bronze→silver→gold) never receive cardholder data, so PCI-DSS scope = zero. Correct answer for "why not" question — not "we're careless," but "data never enters system," which is the actual PCI scoping question interviewers check.

