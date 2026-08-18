import os

from cosmos import ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "brokerage_dwh"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "role_bronze_loader"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=os.getenv("PATH_TO_DBT_VENV"),
)

snowflake_hook_params = {
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "brokerage_dwh"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
}