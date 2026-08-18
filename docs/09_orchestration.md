# 9. Orchestration Layer

**Scope:** the Airflow DAG that drives the platform end to end — running
`ingestion.main` for the current batch, then `dbt build` across
bronze/silver/gold. Covers batch-id derivation, environment/runtime
setup, and the runtime issues hit (and fixed) while standing this up
on Astro + Cosmos.

**Code location:** `airflow-dbt/dags/brokerage_batch_ingestion.py`.

## Table of contents

- [9. Orchestration Layer](#9-orchestration-layer)
  - [Table of contents](#table-of-contents)
  - [9.1 Governing Principle](#91-governing-principle)
  - [9.2 DAG Structure](#92-dag-structure)
  - [9.3 Batch ID Derivation](#93-batch-id-derivation)
  - [9.4 Runtime Environment](#94-runtime-environment)

---

## 9.1 Governing Principle

The orchestration layer's only job is **sequencing and parameterizing**
— it does not duplicate business logic that already lives in
`ingestion/` (bronze loading rules, `02_bronze_design.md`) or `dbt/`
(silver/gold transforms, `04_silver.md`/`05_gold.md`). One DAG run =
one batch: it resolves *which* batch number this run corresponds to,
invokes ingestion for that batch, then runs the full `dbt build`
across bronze → silver → gold once ingestion lands.

---

## 9.2 DAG Structure

`brokerage_batch_ingestion`, four stages:

| Task | Type | Purpose |
|---|---|---|
| `start` | `EmptyOperator` | Marker / fan-in point for monitoring. |
| `get_batch_id` | `PythonOperator` | Resolves the batch number for this scheduled run (§9.3). |
| `run_ingestion` | `BashOperator` | Runs `python -m ingestion.main --batch-id <N>` for the resolved batch. |
| `dbt_build` | `DbtTaskGroup` (Cosmos) | `dbt build` across the full project, tests run after each model (`TestBehavior.AFTER_EACH`). |
| `end` | `EmptyOperator` | Marker / fan-in point for monitoring. |

```
start >> get_batch_id >> run_ingestion >> dbt_build >> end
```

Schedule: `0 2 * * *` (daily, 2 AM UTC). `catchup=True`,
`max_active_runs=1` — batches must land in order, one at a time, same
idempotency contract `03_ingestion.md` §3.4 describes for
`bronze_batch_control`.

---

## 9.3 Batch ID Derivation

**Mapping:** the platform's historical load is Batch 1, loaded once
outside this DAG (out of scope here — see `03_ingestion.md`). This DAG
owns Batch 2 onward: the **first** scheduled run is batch 2, the
**second** scheduled run is batch 3, and so on — one batch per
scheduled day.

**Formula:**
```
batch_id = (data_interval_start - BATCH_START_DATE).days + FIRST_BATCH_ID
```
where `BATCH_START_DATE` is the DAG's `start_date` and
`FIRST_BATCH_ID = 2`.

**Implementation:** resolved in a `PythonOperator`
(`compute_batch_id`), pushed to XCom, and read by the downstream
`BashOperator` via `{{ ti.xcom_pull(task_ids='get_batch_id') }}` — not
computed inline in the bash command's Jinja template. Reason in
ADR-001.

---

## 9.4 Runtime Environment

- **Platform:** Astro CLI + Cosmos (`astronomer-cosmos`) for the
  `DbtTaskGroup`, on `astro-runtime:11.3.0`.
- **Ingestion code mount:** `../ingestion` volume-mapped to
  `/usr/local/airflow/ingestion` in `scheduler`, `webserver`, and
  `triggerer` services. Requires `ingestion/__init__.py` (package,
  importable via `-m`) and an `if __name__ == "__main__":` entrypoint
  in `ingestion/main.py`.
- **dbt project mount:** `./dbt/dbt_project` →
  `/usr/local/airflow/dbt/dbt_project`. Path exposed to the DAG via
  `PATH_TO_DBT_PROJECT` env var.
- **Snowflake connection:** configured as an Airflow Connection (Admin
  → Connections, type Snowflake), referenced by `SNOWFLAKE_CONN_ID` —
  credentials live in the Connection, not in `.env`.
- **Env vars** (`.env`, `docker-compose` `env_file`):
  `SNOWFLAKE_CONN_ID`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DATABASE`,
  `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`,
  `PATH_TO_DBT_VENV`, `PATH_TO_DBT_PROJECT`.
