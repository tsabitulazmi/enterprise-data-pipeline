# Enterprise Data Pipeline: Orchestrating dbt with Airflow on Google BigQuery

## Project Overview
This project is an automated, production-ready data pipeline that transforms and tracks customer subscription data. It leverages **Apache Airflow** for orchestration, **dbt (Data Build Tool)** for data transformation and testing, and **Google BigQuery** as the cloud data warehouse. 

The pipeline ensures data quality and accurately tracks historical changes to user subscriptions over time using **Slowly Changing Dimensions (SCD Type 2)**. The final data is visualized in **Looker Studio** to provide actionable business intelligence.

## Architecture
![Enterprise Data Pipeline Architecture](architecture_diagram.png)

1. **Orchestration:** Dockerized Apache Airflow schedules and triggers the pipeline.
2. **Transformation:** dbt executes SQL models to clean, transform, and snapshot the raw data.
3. **Storage:** Google BigQuery processes the queries and stores the final dimensional models.
4. **Visualization:** Looker Studio connects to BigQuery to visualize active subscriptions and historical trends.

## Key Features
* **Slowly Changing Dimensions (SCD Type 2):** Implemented dbt snapshots to maintain a historical ledger of user subscription upgrades, downgrades, and cancellations.
* **Automated Data Quality Testing:** Engineered dbt tests to enforce strict uniqueness constraints and prevent orphaned records during BigQuery `MERGE` operations.
* **Custom Docker Architecture:** Built a custom Airflow Docker image that isolates Python dependencies.
* **Cloud Security:** Utilized secure Google Cloud Service Account authentication, keeping credentials safely injected via `.env` files and `.gitignore`.

## Technical Challenges Overcome
**1. Dependency Hell (Airflow vs. dbt)**
* **The Problem:** When building the Airflow Worker container, strict package constraints for Airflow 2.9.3 conflicted with the `click` and `google-cloud-aiplatform` dependencies required by `dbt-bigquery`. This caused the Celery worker to fail its health checks and enter an infinite restart loop, leaving tasks permanently queued.
* **The Solution:** Engineered a custom `Dockerfile` to create an isolated Python Virtual Environment (`venv`) strictly for dbt. I then created a symlink to expose the dbt executable to the Airflow user, allowing the two systems to run on the same container without their dependencies ever touching.

**2. BigQuery Merge Conflicts on Historical Data**
* **The Problem:** The initial dbt snapshot failed because the `MERGE` statement was matching multiple source rows to a single target row, caused by users having multiple historical states under the same `user_id`.
* **The Solution:** Redesigned the data model to rely on a true, row-level unique `id` as the primary key for the snapshot strategy, ensuring BigQuery could cleanly update `dbt_valid_to` timestamps without duplication errors.

## Business Intelligence Dashboard
https://datastudio.google.com/reporting/6bcff2dd-8f36-40d3-ad01-0a512c642490