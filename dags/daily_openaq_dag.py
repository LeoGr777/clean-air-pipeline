"""
Airflow DAG for daily tasks (extract, load, transform)
"""

# Imports
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from src.pipeline_runner import run_daily_pipeline

with DAG(
    dag_id="daily_clean_air_etl",
    start_date=pendulum.datetime(2025, 8, 30, tz="Europe/Berlin"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Daily Clean Air ETL DAG

    This DAG runs daily to perform the following steps:
    - Extracts data from the OpenAQ API for endpoints: locations, sensors, measurements
    - Stores data on S3 raw/
    - Transforms the raw data into a clean, usable format
    - Stores data on S3 /processed
    - Loads the transformed data into Snowflake dim and fact tables
    """,
    tags=["daily", "etl", "openaq"],
) as dag:

    run_daily_etl_task = PythonOperator(
        task_id="run_daily_etl", python_callable=run_daily_pipeline
    )
