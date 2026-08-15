# 7. Data Governance, Compliance & Ownership

**Scope:** brokerage data platform (TPC-DI derived), Snowflake + dbt + Python
ingestion. This document is the governance reference for the platform —
who owns what, who can see what, how sensitive data is controlled, how
long it lives, and how the platform proves any of that to an auditor.

It is written to double as an interview/portfolio artifact: each section
maps to a standard data-governance discipline (DAMA-DMBOK terminology),
states the decision made here, and points at the concrete SQL/code that
enforces it. Governance that isn't enforced in the warehouse is just a
wiki page — every control below has a Snowflake object, a dbt config, or
a table behind it, not only a policy statement.

## Table of contents
- [7. Data Governance, Compliance \& Ownership](#7-data-governance-compliance--ownership)
  - [Table of contents](#table-of-contents)
  - [7.1 Governance Model \& Roles](#71-governance-model--roles)
  - [7.2 Ownership \& Stewardship](#72-ownership--stewardship)
  - [7.3 Data Classification](#73-data-classification)
    - [Enforcement — the Snowflake tag is the source of truth](#enforcement--the-snowflake-tag-is-the-source-of-truth)
  - [7.4 Data Lineage](#74-data-lineage)
    - [Row-level lineage — the metadata envelope](#row-level-lineage--the-metadata-envelope)
    - [Table/model-level lineage — the dbt DAG](#tablemodel-level-lineage--the-dbt-dag)
  - [7.5 Metadata Management and Data Catalogs](#75-metadata-management-and-data-catalogs)
  - [7.6 Access Control Paradigms](#76-access-control-paradigms)
  - [7.7 PII Masking \& Privacy Controls](#77-pii-masking--privacy-controls)
  - [7.8 Data Retention and Lifecycle Policies](#78-data-retention-and-lifecycle-policies)
    - [Retention policy](#retention-policy)
    - [Erasure (right-to-be-forgotten) mechanics](#erasure-right-to-be-forgotten-mechanics)
  - [7.9 Operational Auditability](#79-operational-auditability)
    - [Restricted-PII access auditing](#restricted-pii-access-auditing)
    - [Point-in-time reconstruction](#point-in-time-reconstruction)
    - [DQ-as-Control on ingestion](#dq-as-control-on-ingestion)
    - [Operational logging (`logging_setup.py`)](#operational-logging-logging_setuppy)
  - [7.10 Regulatory Compliance Mapping](#710-regulatory-compliance-mapping)
  - [7.11 Data Exposures \& Downstream Consumers](#711-data-exposures--downstream-consumers)
  - [Open items](#open-items)

---

## 7.1 Governance Model & Roles

**Theory:** governance starts with an access model, not a policy
document. This platform uses **Role-Based Access Control (RBAC)** at the
warehouse layer — Snowflake's native `ROLE` object — rather than
per-user grants. RBAC is the standard paradigm for a small platform team;
see [7.6](#76-access-control-paradigms) for why RBAC was chosen over
attribute-based control (ABAC) here.

Five roles, split along two axes: **service account vs. human login**,
and **which layer they may see**. Full grant script: `sql/roles.sql`.

| Role | Identity type | Layer access | Purpose |
|---|---|---|---|
| `role_bronze_loader` | Service account (Python ingestion) | Bronze only — `SELECT`/`INSERT`/`DELETE`, stage `READ`/`WRITE` | The identity `main.py` authenticates as. Never a human credential. Also `INSERT`/`SELECT` on `governance.dq_audit_log`. |
| `role_custodian` | Human | Bronze + Silver + Gold, read-only, plus `governance` | The "sees everything unmasked" role. Masking policies explicitly unmask for this role (and `ACCOUNTADMIN`). In practice the platform owner — trusted for debugging, audits, verification. |
| `role_analyst` | Human | Silver + Gold, read-only | No bronze access — enforced by explicit `REVOKE`, not omission. `restricted_pii` columns render masked. `confidential` columns are visible. Represents any downstream BI/analytics consumer. |
| `role_dbt_prod_ci` | Service account (dbt runs) | Bronze read-only; full build rights on Silver + Gold | The identity that materializes dbt models. Logs to `governance.dq_audit_log` (e.g. failed tests). |
| `role_steward` | Human | Silver + Gold, read-only | Owns *business meaning* — definitions, classification correctness, whether logic still matches the business. `restricted_pii` stays masked, same as `role_analyst`. Recorded as `data_steward` in model `meta`. |

**Design principle — Segregation of Duties (SoD):** `role_bronze_loader`
and `role_dbt_prod_ci` are kept deliberately separate. One identity able
to both write raw bronze *and* rebuild every downstream transformation
would mean a single compromised credential, or a single bug, could
corrupt the pipeline end-to-end with no boundary in between. This is the
same principle an external auditor checks for in a SOX-scoped system —
no single account should be able to both create and certify its own
data.



---

## 7.2 Ownership & Stewardship

**Theory:** DAMA-DMBOK draws a hard line between **data ownership**
(accountable for the asset existing and being correct) and **data
stewardship** (accountable for its business meaning). This platform
encodes that distinction directly in dbt model metadata rather than
leaving it as an org-chart assumption.

Every silver and gold model carries three `meta` fields, set once as a
project-wide default (`+meta` under `models.silver` / `models.gold` in
`dbt_project.yml`) rather than repeated per model — dbt merges `meta`
down through folder-level config, including subfolders
(`archetype_a/`, `fact_tables/`, etc.). Any individual model can still
override a single field in its own `schema.yml`. `sources.yml` has no
equivalent inheritance mechanism, so `bronze`'s `meta` is set once
directly on the source.

| Field | Meaning | Current value |
|---|---|---|
| `owner` | Team accountable for the layer overall | `brokerage-data-platform` |
| `data_steward` | Owns business meaning — definitions, classification correctness, whether logic still matches the business | `role_steward` |
| `technical_owner` | Owns the pipeline/code — who gets paged when a load or model build breaks | `role_custodian` |

Today `role_steward` and `role_custodian` are the same person wearing
two hats on a one-person project. Both roles are still recorded
distinctly — the grants, docs, and `meta` values already reflect the
split, so onboarding a second person only requires re-pointing one
field, not redesigning the model.



---

## 7.3 Data Classification

**Theory:** classification is the prerequisite for every downstream
control (masking, retention, regulatory scoping). The governing rule
here: **classify at the point data first lands, not at the point it's
first "understood."** Bronze carries the exact same raw PII and
financial values silver does — a customer's name is exactly as
sensitive in raw CDC form as it is after SCD2 modeling. What silver
adds is *business meaning* (current status, deduped state), not
*sensitivity* — so classification is applied starting at ingestion.

Four levels, ascending sensitivity:

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

Every PII/financial column across bronze, silver, and gold is tagged
directly at the warehouse level via `ALTER TABLE ... MODIFY COLUMN ...
SET TAG` (`sql/classification_tags.sql`, covers bronze archetypes A–E,
silver, gold). This is deliberate: the tag is queryable and enforceable
regardless of which tool touches the column — dbt, a raw SQL client, a
BI tool — it does not depend on anyone reading documentation first.

```sql
SELECT OBJECT_NAME, COLUMN_NAME, TAG_VALUE
FROM TABLE(
    brokerage_dwh.information_schema.TAG_REFERENCES_ALL_COLUMNS(
        'brokerage_dwh.bronze.bronze_customer', 'table'
    )
) LIMIT 5;
```

| OBJECT_NAME | COLUMN_NAME | TAG_VALUE |
|---|---|---|
| BRONZE_CUSTOMER | C_ID | confidential |
| BRONZE_CUSTOMER | C_TIER | confidential |
| BRONZE_CUSTOMER | C_ST_ID | internal |
| BRONZE_CUSTOMER | C_LCL_TX_ID | internal |
| BRONZE_CUSTOMER | C_NAT_TX_ID | internal |

**Documentation mirror:** `meta.classification` is set on the same
columns in `sources.yml` / `_silver__models.yml` / `_gold__models.yml`,
so classification is visible directly in `dbt docs generate` output
next to each column's definition. **The Snowflake tag is authoritative;
the YAML is a mirror of it, not a second source of truth** — if the two
ever disagree, the tag wins and the YAML is stale.


---

## 7.4 Data Lineage

**Theory:** lineage answers "where did this value come from, and what
touched it on the way here" — required both for debugging (trace a bad
gold number back to its source row) and for regulatory audit (SOX,
GDPR both require the ability to reconstruct provenance). This platform
implements lineage at two levels: **column/table lineage** (dbt's
native DAG) and **row-level lineage** (the metadata envelope carried on
every row).

### Row-level lineage — the metadata envelope

Every bronze row carries:

| Column | Purpose |
|---|---|
| `_batch_id` | Which load batch produced this row |
| `_source_file` | Exact filename ingested |
| `_loaded_at` | Ingestion wall-clock time |
| `_row_hash` | Deterministic hash of business columns — dedup/QA signal |
| `_dq_errors` | Structured cast-failure evidence (see `06_data_quality.md`, §6.1) |

This envelope survives into silver as `_batch_id` (carried forward for
lineage) and, on the quasi-CDC/CDC models, `_cdc_flag`/`_cdc_dsn` where
relevant. `silver_trade_history` additionally carries a `_source_model`
column recording which of the two unified bronze sources
(`bronze_trade_history` vs. `bronze_trade`) produced a given row — an
explicit lineage marker rather than an implicit proxy like `_batch_id =
1`. `fact_trade_history` mirrors this as `source_model` in gold.

### Table/model-level lineage — the dbt DAG

Every dbt model's `ref()`/`source()` graph is a complete, queryable
lineage map from raw file to gold table, visualized automatically by
`dbt docs generate`. Because every column on a gold table is required
to trace to a silver column — either directly or via a documented
resolution join (see `05_gold.md`'s governing principle) — the DAG is
not just a dependency graph, it's an audit trail: "how was this gold
column derived" always has a one-hop or two-hop answer, never a black
box.


---

## 7.5 Metadata Management and Data Catalogs

**Theory:** metadata management is the discipline of keeping
*data about the data* — definitions, classification, ownership, test
coverage, freshness — as accurate and discoverable as the data itself.
Without it, governance controls exist in code but are invisible to
anyone who isn't reading SQL DDL.

This platform's catalog is **`dbt docs generate`**, backed by:

- **Descriptions** on every source and model, in `sources.yml`,
  `_silver__models.yml`, `_gold__models.yml` — human-readable
  definitions, not just column names.
- **Classification metadata** (`meta.classification`), mirroring the
  Snowflake tags from [7.3](#73-data-classification), rendered next to
  each column.
- **Ownership metadata** (`meta.owner` / `data_steward` /
  `technical_owner`), from [7.2](#72-ownership--stewardship).
- **Test coverage**, rendered per column (`not_null`, `unique`,
  `accepted_values`, `relationships` — see `06_data_quality.md`, §6.1) so a
  reader can see which columns are actually validated, not just
  documented.
- **Lineage graph**, from [7.4](#74-data-lineage).
- **Exposures** ([7.11](#211-data-exposures--downstream-consumers)),
  which extend the catalog past the warehouse boundary into actual
  downstream consumers (dashboards, ML models, reverse-ETL syncs).

This is a **passive/embedded catalog** (metadata lives with the code
that produces it, generated on build) rather than an **active catalog**
(a separate tool like Collibra/Alation/Atlan/DataHub scanning the
warehouse independently). For a platform of this size, the dbt-native
catalog keeps documentation and code from drifting apart — there is no
second system to keep in sync. Revisit only if the org needs
cross-platform discovery (data outside dbt's reach) or business-user
self-service search, which are the actual differentiators of a
dedicated catalog product.



---

## 7.6 Access Control Paradigms

**Theory:** two dominant paradigms exist for controlling who can see
what: **RBAC** (permissions attached to a role, users assigned to
roles) and **ABAC** (permissions evaluated dynamically against
attributes of the user, the data, and the context — e.g. "allow if
`user.department = data.department`"). RBAC is simpler to audit (a
fixed, enumerable set of roles and grants); ABAC is more expressive but
harder to reason about and test.

**Decision:** RBAC, via Snowflake's native `ROLE` grant model
([7.1](#71-governance-model--roles)), layered with
**policy-based dynamic masking** ([7.7](#77-pii-masking--privacy-controls))
for the one case that genuinely needs a runtime, condition-based
decision (unmask PII only for specific roles). This is a hybrid, not
pure RBAC — the masking policy's `CURRENT_ROLE()` check is a narrow,
deliberate use of an attribute-style condition inside an otherwise
role-based model, not a full ABAC system.

**Reason RBAC over full ABAC here:** five roles across three layers is
small enough to enumerate, grant, and audit directly (`SHOW GRANTS TO
ROLE ...`). A full ABAC system (row-level attribute policies, e.g.
region- or department-scoped access) would add real value once the
consumer base is heterogeneous enough to need it — not yet the case for
a single analyst/steward/custodian population.

Access is enforced at three points, from broadest to narrowest:

1. **Layer-level** — `GRANT`/`REVOKE` on schemas/tables per role
   ([7.1](#71-governance-model--roles)).
2. **Column-level** — masking policies on `restricted_pii` columns
   ([7.7](#77-pii-masking--privacy-controls)).
3. **Row-level** — not currently implemented (no row-access policy in
   use); flagged as an open item below since a future multi-tenant or
   multi-region requirement would need it.



---

## 7.7 PII Masking & Privacy Controls

Reusable masking policies live in `governance`
(`sql/masking_policy`), applied per column rather than duplicated per
table:

- **Policies:** `mask_pii_string`, `mask_pii_date`, `mask_pii_numeric`
  — applied via `ALTER TABLE ... SET MASKING POLICY` on every
  `restricted_pii` column, across bronze, silver, and gold.
- **Enforcement condition:** `CURRENT_ROLE()`. `role_custodian` and
  `ACCOUNTADMIN` see real values; every other role — including
  `role_analyst` — sees the masked form. This is evaluated automatically
  on every query; no per-query logic needed on the consumer side.
- **Masked representation:** `mask_pii_string` returns `***MASKED***`.
  `mask_pii_numeric`/`mask_pii_date` return `NULL`, since a numeric or
  date value has no natural masked string form.

**Example — `silver_hr.middle_initial`:**

```sql
SELECT middle_initial FROM brokerage_dwh.silver.silver_hr LIMIT 5;
```

| As `role_analyst` (masked) | As `role_custodian` (unmasked) |
|---|---|
| `***MASKED***` | `R` |
| `***MASKED***` | `X` |
| `***MASKED***` | `N` |
| `***MASKED***` | `V` |
| `***MASKED***` | `I` |



---

## 7.8 Data Retention and Lifecycle Policies

**Theory:** a lifecycle policy states, for every class of data, how
long it is kept, in what form, and what happens at end-of-life
(deletion, anonymization, archival). GDPR's storage-limitation
principle and the right to erasure both require this to be a documented,
executable policy — not an ad hoc decision made at deletion time.

### Retention policy

Raw PII is retained for **90 days** at the bronze layer, to support
recovery, debugging, and controlled reprocessing, balanced against
unnecessary long-term exposure of raw identity data:

```sql
ALTER TABLE bronze_customer
SET DATA_RETENTION_TIME_IN_DAYS = 90;
```

Silver/gold retention is effectively indefinite by current design (SCD2
history is kept forever — see [7.10](#210-regulatory-compliance-mapping),
GDPR-style entry, for the accepted tension this creates with erasure
rights).

### Erasure (right-to-be-forgotten) mechanics

`erasure_log` is the permanent record of what was erased, when, and by
whom:

| Column | Purpose |
|---|---|
| `erasure_id` | Unique identifier per request |
| `customer_id` | Subject of the request |
| `requested_at` / `erased_at` | Request vs. execution timestamp |
| `reason` | Why erasure was requested |
| `status` | Current state of the request |
| `affected_layers` | Which layers were touched |
| `requested_by` | Person/system that made the request |
| `notes` | Free-text context |

**Mechanism:** PII columns are set to `NULL` in the affected layers;
rows themselves are **never deleted**. Deleting rows would break
referential integrity and defeat point-in-time reconstruction
([7.9](#79-operational-auditability)). Nullifying preserves the
row's structural role (foreign keys, grain, historical counts) while
removing the actual personal data.

```sql
UPDATE silver_customer
SET
    email = NULL,
    phone = NULL,
    tax_id = NULL,
    ...
WHERE customer_id = 123;
```

**Lifecycle decision, stated explicitly:** this is *anonymization
in place*, not *deletion* or *archival-then-purge*. The trade-off is
recorded in `erasure_log` itself — an auditor can prove a specific
request was actioned, without the platform losing the ability to
answer "how many accounts existed on date X."



---

## 7.9 Operational Auditability

**Theory:** auditability is the ability to answer, after the fact,
*what happened, when, to whom, and who did it* — without relying on
memory or ad hoc investigation. This platform separates three distinct
evidence trails, each answering a different auditability question, so
that none of them get diluted into the others.

| | `_dq_errors` | DQ audit trail (`governance.dq_audit_log`) | Operational log |
|---|---|---|---|
| **Question answered** | Was this specific value trustworthy? | Did this check pass, batch-over-batch? | What did the process do, in what order? |
| **Where** | column on every bronze table | Snowflake table | stdout / optional file |
| **Grain** | per row, per column | per batch, per check | per process run |
| **Lifetime** | permanent, lives with the row | permanent, queryable | transient, run-scoped |
| **Acted on by** | silver (fix/flag/reject) | auditor/reviewer | developer |
| **Written by** | `_safe_cast()` / `_pack_dq_errors()` | `log_dq_event()` | `log.info`/`log.warning` (stdlib `logging`) |

Full mechanics of `_dq_errors` and DQ-as-Control checks are documented
in `06_data_quality.md`; this section covers the **access-auditing**
half of operational auditability specifically.

### Restricted-PII access auditing

Snowflake `ACCESS_HISTORY` is queried to audit access to every
`restricted_pii` column — e.g. for `silver_hr`: `first_name`,
`last_name`, `middle_initial`, `phone`. The audit query (scoped to the
last 30 days, in `sql/restricted_pii_access_history.sql`) returns time,
user, query ID, and SQL text for any query that touched at least one of
these columns — answering directly: **who accessed restricted PII, when,
and with what query.**

### Point-in-time reconstruction

`silver_account` (and `silver_customer`) are SCD2-modeled with
`valid_from_date`/`valid_to_date`, which makes "what did this account
look like on a given date" a direct, auditable query rather than a
reconstruction exercise:

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
  AND (valid_to_date >= $as_of_date OR valid_to_date IS NULL)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id
    ORDER BY valid_from_date DESC
) = 1
ORDER BY account_id;
```

### DQ-as-Control on ingestion

`governance.dq_audit_log` is a structured, permanent DQ evidence table
— it replaces `print()`-only reconciliation warnings. `log_dq_event()`
(in `main.py`) inserts one row per check result in its own transaction:
commit on success, rollback and raise on failure, since a broken
audit-log insert must never silently disappear.

Reconciliation (audit file `RowCount` vs. actual bronze rows loaded)
logs **every** outcome, not only mismatches — this is deliberate:
logging passes too means the table proves reconciliation *coverage*
per batch, not just failures.

| Outcome | `check_type` | `severity` |
|---|---|---|
| Counts match | `reconciliation_check` | `PASS` |
| Counts differ | `reconciliation_mismatch` | `WARNING` |
| Audit expects a source, none was loaded | `reconciliation_mismatch` | `WARNING` |

Mismatches stay non-fatal — a mismatch can have a legitimate
explanation, so it's surfaced for human review rather than aborting the
batch automatically.

### Operational logging (`logging_setup.py`)

Separate again from the two evidence tables above — this is
process/progress output, not business evidence, and is **not** written
to Snowflake. Console handler always on; file handler added when
`--log-file` is passed. `LoggerAdapter` injects `batch_id` into every
record automatically.



---

## 7.10 Regulatory Compliance Mapping

**Theory:** compliance mapping ties abstract regulatory obligations to
concrete tables/columns/controls in the actual model — a regulation
that isn't mapped to a specific object is not actually enforced, just
referenced.

| Regime | In scope? | Mapped objects | Control need |
|---|---|---|---|
| **SOX-style** (financial reporting integrity) | Yes | `fact_trade`, `fact_trade_history`, `silver_trade` | Integrity + audit trail + no double-count risk on financial measures (commission, balance). The ADR-002/ADR-009 split (silver + gold: one row latest-state, one row full status-lineage) exists specifically to stop the fan-trap where `SUM(commission)` multiplies by transition count. `_row_hash`/`_batch_id`/`_cdc_dsn` ordering gives a traceable lineage — an auditor can reconstruct "what value, when, from what source event." SOX-style control here means financial reporting accuracy and an immutable audit trail, not raw storage security. |
| **GDPR-style** (personal data protection) | Yes | `silver_customer` / `dim_customer` | Holds name, address, email, DOB, tax ID, phone. SCD2 (day-grain, tracked columns include name/address/email) keeps history forever by design — a direct, accepted tension against the erasure right and data-minimization principle. Carried-only fields (phone, DOB, tax ID) are still personal data even though not versioned for change-tracking — still in scope. Control need: retention policy + deletion/anonymization procedure over SCD2 history ([7.8](#78-data-retention-and-lifecycle-policies)), access control on PII columns ([7.6](#76-access-control-paradigms)/[7.7](#77-pii-masking--privacy-controls)), and a documented lawful basis for retaining full history (business need: identity/location/status audit, per spec). |
| **PCI-DSS** (cardholder data) | **No** | — | No PAN, card number, CVV, or expiry field anywhere in the source dictionary or model. `CashTransaction`/`fact_cashtransaction` carry amount + type only, never a payment instrument. The pipeline never receives cardholder data end-to-end, so PCI-DSS scope is zero — the correct scoping answer is "the data never enters the system," not "we handle it carefully." |

---

## 7.11 Data Exposures & Downstream Consumers

`exposures.yml` declares downstream consumers of the gold layer so dbt
can render them in the DAG/docs site, and `dbt build --select
+exposure:*` can validate that every model an exposure depends on still
exists and builds clean. Exposures extend lineage
([7.4](#74-data-lineage)) past the warehouse boundary — governance
doesn't stop at "who can query the table," it also tracks "what actually
consumes this table downstream."

| Exposure | Type | Status | Depends on |
|---|---|---|---|
| `power_bi_dashboard` | `dashboard` | **Real** | All 17 gold models (full star schema) |
| `ml_trade_behavior_model` | `ml` | Placeholder | `fact_trade`, `fact_holding`, `fact_market_history`, `dim_customer`, `dim_security` |
| `reverse_etl_crm_sync` | `application` | Placeholder | `dim_customer`, `dim_account`, `dim_prospect` |

`ml_trade_behavior_model` and `reverse_etl_crm_sync` are declared as an
exercise — nothing downstream actually consumes those models today.
An exposure only makes sense once real gold models exist to point at,
which is why this section comes after the star schema, not before it.

---

## Open items

Tracked honestly rather than presented as done — the practice a senior
reviewer looks for:

1. **Row-level access control** not implemented ([7.6](#76-access-control-paradigms))
   — no current requirement, but flagged for a future multi-tenant or
   multi-region scope.
2. **Prospect-to-customer matching rule** unconfirmed (TPC-DI spec
   Clause 4.5.x) — blocks populating `dim_prospect.customer_sk`
   (see `05_gold.md` ADR-004).
3. **Reconciliation thresholds** (`06_data_quality.md` §6.3)
   are working defaults, not confirmed business SLAs — revisit once
   real batch volumes are known.
4. **Active data catalog** (Collibra/Alation/Atlan/DataHub-class tool)
   not adopted — current dbt-native catalog ([7.5](#75-metadata-management-and-data-catalogs))
   is sufficient at this scale; revisit if cross-platform discovery or
   business-user self-service search becomes a requirement.
