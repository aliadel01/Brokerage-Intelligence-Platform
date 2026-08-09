# Governance, Compliance and Ownership
## Table of Contents
  1. [Roles & Access Control](#roles--access-control)
  2. [Data Classification](#data-classification)
  3. [PII Masking Policies](#pii-masking-policies)
  4. [DQ-as-Control on Ingestion](#dq-as-control-on-ingestion)
  5. [Open items](#open-items)


## Roles & Access Control

Four roles, split along two axes: **service account vs. human login**, and
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
 

## Open items

- Metadata envelope columns (`_batch_id`, `_source_file`, `_loaded_at`,
  `_row_hash`, `_dq_errors`, `_cdc_flag`, `_cdc_dsn`) are intentionally
  untagged for now — pipeline metadata, not source-classified data.
  Revisit if a stricter policy requires tagging these as `internal` too.
- `dq_audit_log` is defined but not yet wired into `main.py` — the
  reconciliation check still only `print()`s warnings; inserting into
  `governance.dq_audit_log` from `run_batch()` is the next step.
- `role_custodian`/`role_analyst` are not yet bound to real human users
  (`GRANT ROLE ... TO USER ...` lines are placeholders) — needs actual
  Snowflake usernames filled in.
- `numbercars`, `numberchildren`, `age`, `maritalstatus`, `ownorrentflag`,
  `numbercreditcards` on `bronze_prospect` have no explicit
  `data_classification` tag yet (only `income`/`creditrating`/
  `networth`/`employer`/`agencyid` and the name/address/contact fields
  are tagged in `classification_tags.sql`). Silver/gold carry these as
  `restricted_pii` by inference, not by a matching bronze tag — revisit
  if full bronze↔silver↔gold parity is required later.