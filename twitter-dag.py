"""
Airflow DAG: Twitter ETL Pipeline
Goal:
- Define a scheduled workflow using Apache Airflow
- Run the Twitter ETL Python function once per day
- Handle retries and basic failure behavior
Input and output behavior remain the same as the original script.
"""

# -----------------------------
# Standard library imports
# -----------------------------
from datetime import datetime, timedelta
# datetime is used for defining start dates
# timedelta is used for scheduling and retry delays

# -----------------------------
# Airflow core imports
# -----------------------------
from airflow import DAG
# DAG is the core Airflow object that defines a workflow

from airflow.operators.python_operator import PythonOperator
# PythonOperator is used to run a Python function as a task

from airflow.utils.dates import days_ago
# days_ago is a helper for relative start dates (not used directly here)

# -----------------------------
# Local ETL function import
# -----------------------------
from twitter_etl import run_twitter_etl
# This imports the ETL function that handles Twitter data extraction and transformation

# -----------------------------
# Default DAG arguments
# -----------------------------
# These settings apply to all tasks in the DAG unless overridden
default_args = {
    'owner': 'airflow',                 # Owner shown in the Airflow UI
    'depends_on_past': False,            # Each run is independent
    'start_date': datetime(2020, 11, 8), # First date the DAG is allowed to run
    'email': ['airflow@example.com'],    # Notification email (placeholder)
    'email_on_failure': False,           # Do not email on failure
    'email_on_retry': False,             # Do not email on retry
    'retries': 1,                        # Retry once if the task fails
    'retry_delay': timedelta(minutes=1)  # Wait 1 minute before retrying
}

# -----------------------------
# DAG definition
# -----------------------------
# This creates the DAG object that Airflow will register
dag = DAG(
    dag_id='twitter_dag',                        # Unique DAG name in Airflow
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),         # Run once per day
)

# -----------------------------
# Task definition
# -----------------------------
# This task runs the Twitter ETL function
run_etl_task = PythonOperator(
    task_id='complete_twitter_etl',   # Task name shown in the Airflow UI
    python_callable=run_twitter_etl,  # Function that performs the ETL work
    dag=dag,
)

# -----------------------------
# DAG execution order
# -----------------------------
# Since there is only one task, no dependencies are needed
run_etl_task
