# ============================================================
# dags/weather_pipeline_dag.py — Apache Airflow DAG
# 
# This is the production-ready Airflow version of scheduler.py
# Use this when you set up Apache Airflow (Docker or pip install)
#
# Schedule: Every hour (@hourly)
# Tasks: ingest >> transform >> load >> validate
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default args — applied to all tasks
default_args = {
    "owner":            "shreya_reddy",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,                         # Retry failed tasks 3 times
    "retry_delay":      timedelta(minutes=5),      # Wait 5 min between retries
}

# DAG definition
dag = DAG(
    dag_id="weather_data_pipeline",
    default_args=default_args,
    description="Hourly weather data ingestion, transformation, and loading pipeline",
    schedule_interval="@hourly",                   # Runs every hour
    catchup=False,                                 # Don't backfill missed runs
    tags=["weather", "etl", "data-engineering"],
)


# ── Task functions ────────────────────────────────────────────

def task_ingest(**context):
    """Fetch weather data from OpenWeatherMap API and save raw JSON."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from src.ingestion import run_ingestion
    from src.db_loader import log_pipeline_run

    saved = run_ingestion()
    log_pipeline_run("ingestion", "SUCCESS", len(saved))
    # Push file list to XCom for downstream tasks
    context["ti"].xcom_push(key="saved_files", value=saved)


def task_transform(**context):
    """Parse, clean, detect anomalies, and aggregate raw data."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from src.transformation import run_transformation
    from src.db_loader import log_pipeline_run

    records_df, agg_df = run_transformation()
    log_pipeline_run("transformation", "SUCCESS", len(records_df))

    # Serialize to pass between tasks via XCom (small datasets only)
    context["ti"].xcom_push(key="record_count", value=len(records_df))


def task_load(**context):
    """Load processed data into database."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    import glob, os, pandas as pd
    from src.db_loader import run_db_load, log_pipeline_run
    from config import PROCESSED_DATA_PATH

    # Load latest processed files
    record_files = sorted(glob.glob(os.path.join(PROCESSED_DATA_PATH, "weather_records_*.csv")))
    agg_files    = sorted(glob.glob(os.path.join(PROCESSED_DATA_PATH, "weather_aggregates_*.csv")))

    if not record_files or not agg_files:
        raise FileNotFoundError("No processed files found — transformation may have failed")

    records_df = pd.read_csv(record_files[-1])
    agg_df     = pd.read_csv(agg_files[-1])
    run_db_load(records_df, agg_df)


def task_validate(**context):
    """Validate data landed correctly — fail DAG if anomalies in schema."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from src.db_loader import query_summary

    summary = query_summary()
    if summary.empty:
        raise ValueError("Validation failed — no data found in database after load")

    print("✅ Validation passed:\n", summary.to_string())


# ── Task definitions ──────────────────────────────────────────

ingest_task = PythonOperator(
    task_id="ingest_weather_data",
    python_callable=task_ingest,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_and_detect_anomalies",
    python_callable=task_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_database",
    python_callable=task_load,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_pipeline_output",
    python_callable=task_validate,
    dag=dag,
)

# ── Task dependencies (DAG structure) ────────────────────────
# ingest >> transform >> load >> validate
ingest_task >> transform_task >> load_task >> validate_task
