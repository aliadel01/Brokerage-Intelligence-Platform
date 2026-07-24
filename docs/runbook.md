## Table of Contents
1. [Setup TPC-DI & Generate Sample Data](#setup-tpc-di--generate-sample-data)
2. [Initialize Airflow with dbt using Astro](#initialize-airflow-with-dbt-using-astro)



## Setup TPC-DI & Generate Sample Data
> You should have Java 8 installed and set in your local environment variables to run the TPC-DI data generator.
1. Download the TPC-DI data generator from the official [TPC website](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY). 
2. Extract the downloaded ZIP file and run `java -jar DIGen.jar -sf 3 -o ../data` in the terminal. 

## Initialize Airflow with dbt using Astro
After installing the Astro CLI, you can initialize your Airflow project with dbt by following these steps:
- Run `astro dev init`
- Add to requirements.txt the following packages:
    ```bash
    astronomer-cosmos
    apache-airflow-providers-snowflake
    pandas
    elementary-data[snowflake]
    astro-run-dag 
    ```
- Add `dbt-snowflake` to dbt-requirements.txt.
- Add [docker-compose.override.yml](../airflow/docker-compose.override.yml).
- Add to Dockerfile:
        ```bash
        FROM quay.io/astronomer/astro-runtime:11.3.0

        # install dbt into a venv to avoid package dependency conflicts
        WORKDIR "/usr/local/airflow"
        COPY dbt-requirements.txt ./
        RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
            pip install --no-cache-dir dbt-snowflake && deactivate
        ```
- Install dbt using `pip install -r dbt-requirements.txt` -> it will install dbt-snowflake and its dependencies like `dbt-core`.
- Run `dbt debug` to verify the dbt connection to Snowflake.