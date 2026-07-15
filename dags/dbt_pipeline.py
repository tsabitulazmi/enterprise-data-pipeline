from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random

# The Google Cloud BigQuery client
from google.cloud import bigquery

# Define the default settings for the pipeline
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def extract_and_load_to_bq():
    """Simulates extracting data from an API and loading it to BigQuery."""
    # Initialize the BigQuery client (Airflow uses your gcp_key.json automatically)
    client = bigquery.Client.from_service_account_json('/opt/airflow/dbt_project/gcp_key.json')
    
    # TODO: Change this to your actual GCP Project ID and Dataset!
    table_id = "gcp-first-project-493504.dbt_subscription.raw_subscription"
    
    # Fetch the current max(id) from BigQuery
    query = f"SELECT COALESCE(MAX(id), 0) AS max_id FROM `{table_id}`"
    query_job = client.query(query)
    results = list(query_job.result())
    current_max_id = results[0]["max_id"]

    # Options to pick from randomly
    statuses = ['active', 'cancelled', 'upgraded', 'expired']
    plan_prices = {
        "Basic": 9.99,
        "Pro": 29.99,
        "Enterprise": 99.99,
    }
    user_pool = list(range(101, 121))
    
    # Generates 10 random subscription updates per run
    new_records = []
    for i in range(20):
        next_id = current_max_id + i + 1
        plan = random.choice(list(plan_prices.keys()))

        new_records.append({
            "id": next_id,
            "user_id": user_pool[i],
            "plan_name": plan,
            "status": random.choice(statuses),
            "price": plan_prices[plan],
            "updated_at": datetime.utcnow().isoformat()
        })
    
    # Load the data directly into BigQuery
    errors = client.insert_rows_json(table_id, new_records)
    if errors:
        raise Exception(f"Failed to load data to BigQuery: {errors}")
    print(f"Successfully loaded new record to {table_id}: {new_records}")
    
# Create the DAG structure
with DAG(
    'enterprise_elt_pipeline',
    default_args=default_args,
    description='Automated daily pipeline for Slowly Changing Dimensions and Data Quality',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Task 1: Extract and Load (E & L)
    extract_load_task = PythonOperator(
        task_id='extract_and_load_api_data',
        python_callable=extract_and_load_to_bq,
        retries=3,                            # Retry up to 3 times
        retry_delay=timedelta(seconds=10),
    )

    # Task 2: dbt Snapshot (Track History)
    dbt_snapshot_task = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/dbt_project && dbt snapshot --profiles-dir .',
        retries=3,                            # Retry up to 3 times
        retry_delay=timedelta(seconds=10),
    )

    # Task 3: dbt Run (Transform Data)
    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
        retries=3,                            # Retry up to 3 times
        retry_delay=timedelta(seconds=10),
    )
    
    # Task 4: dbt Test (Quality Checks)
    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
        retries=3,                            # Retry up to 3 times
        retry_delay=timedelta(seconds=10),
    )

    # Set the strict dependency order
    extract_load_task >> dbt_snapshot_task >> dbt_run_task >> dbt_test_task