"""
Airflow DAG for weekly tasks (extract, load, transform)
"""

# Imports
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from src.pipeline_runner import run_weekly_pipeline

with DAG(
    dag_id="weekly_clean_air_etl",
    start_date=pendulum.datetime(2025, 8, 30, tz="Europe/Berlin"),
    schedule="@weekly",
    catchup=False,
    doc_md="""
    ### Weekly Snowflake Load DAG

    This DAG runs weekly to perform the following steps:
    - Extracts data from the OpenAQ API for endpoints: parameters
    - Stores data on S3 raw/
    - Transforms the raw data into a clean, usable format
    - Stores data on S3 /processed
    - Loads the transformed data into Snowflake dim and fact tables
    """,
    tags=["weekly", "load", "snowflake"],
) as dag:

    run_weekly_load_task = PythonOperator(
        task_id="run_weekly_etl", python_callable=run_weekly_pipeline
    )
