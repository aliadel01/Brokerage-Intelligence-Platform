"""
Brokerage Data Platform — Batch Ingestion DAG

Schedule: Daily
Purpose: run ingestion for batch N, then dbt build bronze/silver/gold

Batch mapping: batch_id = 2 on first scheduled run, 3 next run, etc.
(Batch 1 assumed pre-loaded / historical load, handled outside this DAG.)

Data Flow:
- start
- run ingestion.main for current batch_id
- dbt build (DbtTaskGroup)
- end
"""

import os
from datetime import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig, TestBehavior

from config.dbt_config import profile_config, execution_config

BATCH_START_DATE = pendulum.datetime(2017, 7, 7, tz="UTC")
BATCH_END_DATE = pendulum.datetime(2017, 7, 9, tz="UTC") # we only have data for 2 days + historical data, so limit to that
FIRST_BATCH_ID = 2


def compute_batch_id(**context):
    data_interval_start = context["data_interval_start"]  # already tz-aware
    days_elapsed = (data_interval_start - BATCH_START_DATE).days
    print(f"data_interval_start: {data_interval_start}, days_elapsed: {days_elapsed}")
    return days_elapsed + FIRST_BATCH_ID


with DAG(
    dag_id="brokerage_batch_ingestion",
    description="Daily batch ingestion (ingestion.main) + dbt build for bronze/silver/gold",
    doc_md=__doc__,
    start_date=BATCH_START_DATE,
    end_date=BATCH_END_DATE,
    schedule="0 2 * * *",  # 2 AM daily
    catchup=True,
    max_active_runs=1,
    tags=["brokerage", "tpc-di", "batch"],
) as dag:

    # Task 1
    start = EmptyOperator(task_id="start")

    get_batch_id = PythonOperator(
        task_id="get_batch_id",
        python_callable=compute_batch_id,
    )

    # Task 2: run ingestion for this batch
    # batch_id = days since BATCH_START_DATE + FIRST_BATCH_ID
    # run 1 (day 0) -> batch 2, run 2 (day 1) -> batch 3, etc.
    run_ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command=(
            "cd /usr/local/airflow && "
            "python -m ingestion.main --batch-id "
            "{{ ti.xcom_pull(task_ids='get_batch_id') }}"
        ),
    )

    # Task 3: dbt build (bronze -> silver -> gold, all tests inline)
    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(os.getenv("PATH_TO_DBT_PROJECT")),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={
            "indirect_selection": "cautious",
        },
        default_args={"retries": 0},
    )

    # Task 4
    end = EmptyOperator(task_id="end")

    start >> get_batch_id >> run_ingestion >> dbt_build >> end
