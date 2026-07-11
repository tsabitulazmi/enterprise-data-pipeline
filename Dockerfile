FROM apache/airflow:2.9.3

# Create an isolated virtual environment just for dbt
RUN python -m venv dbt_venv && \
    # Install dbt inside the bubble (ignoring Airflow's constraints)
    dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.11.3 && \
    # Create a shortcut (symlink) so Airflow can run the dbt command globally
    ln -s /opt/airflow/dbt_venv/bin/dbt /home/airflow/.local/bin/dbt