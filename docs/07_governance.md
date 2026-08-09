# Governance, Compliance and Ownership
## Table of Contents
- [Governance, Compliance and Ownership](#governance-compliance-and-ownership)
  - [Table of Contents](#table-of-contents)
  - [Roles \& Access Control](#roles--access-control)
  - [Open items](#open-items)

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
   omission). PII columns appear masked (`***MASKED***` / `NULL`) to this
   role, since it falls outside the unmask condition in the masking
   policies. Represents any downstream consumer who needs the modeled
   data but has no business reason to see raw bronze or unmasked PII.

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





## Open items

- Silver/gold classification not yet applied — will re-tag (or inherit
  via lineage) once silver models are finalized.
- Metadata envelope columns (`_batch_id`, `_source_file`, `_loaded_at`,
  `_row_hash`, `_dq_errors`, `_cdc_flag`, `_cdc_dsn`) are intentionally
  untagged for now — pipeline metadata, not source-classified data.
  Revisit if a stricter policy requires tagging these as `internal` too.
- `dq_audit_log` is defined but not yet wired into `main.py` — the
  reconciliation check still only `print()`s warnings; inserting into
  `governance.dq_audit_log` from `run_batch()` is the next step.
- Masking policies applied to `silver_customer` only so far; the
  commented-out `gold.dim_customer` block in `governance.sql` needs
  activating once that table is actually built.
- `role_custodian`/`role_analyst` are not yet bound to real human users
  (`GRANT ROLE ... TO USER ...` lines are placeholders) — needs actual
  Snowflake usernames filled in.