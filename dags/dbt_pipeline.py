from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default settings for the pipeline
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG structure
with DAG(
    'enterprise_dbt_pipeline',
    default_args=default_args,
    description='Automated daily pipeline for Slowly Changing Dimensions and Data Quality',
    schedule_interval='@daily', # Runs once a day at midnight
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['portfolio', 'dbt', 'bigquery'],
) as dag:

    # Task 1: Update the SCD Type 2 tables in BigQuery
    run_dbt_snapshot = BashOperator(
        task_id='run_dbt_snapshot',
        bash_command='cd /opt/airflow/dags/subscription_dw && dbt snapshot --profiles-dir .'
    )

    # Task 2: Run data quality tests on the newly updated data
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dags/subscription_dw && dbt test --profiles-dir .'
    )

    # Define the order of execution (Snapshot FIRST, then Test)
    run_dbt_snapshot >> run_dbt_tests