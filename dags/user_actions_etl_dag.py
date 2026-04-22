from datetime import timedelta
from pathlib import Path
from io import StringIO
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import ETL_Pipeline
from Config import RAW_FILE_PATH


def extract_task(**context):
    raw_data = ETL_Pipeline.read_raw_logs(RAW_FILE_PATH)
    source_file_name = Path(RAW_FILE_PATH).name

    context["ti"].xcom_push(key="raw_data", value=raw_data)
    context["ti"].xcom_push(key="source_file_name", value=source_file_name)


def transform_task(**context):
    raw_data = context["ti"].xcom_pull(task_ids="extract_raw_logs", key="raw_data")
    source_file_name = context["ti"].xcom_pull(task_ids="extract_raw_logs", key="source_file_name")

    (
        dim_users,
        dim_actions,
        fact_user_actions,
        rejected_records_df,
        count_summary,
    ) = ETL_Pipeline.transform_logs_efficient(raw_data, source_file_name)

    context["ti"].xcom_push(key="dim_users", value=dim_users.to_json(orient="records"))
    context["ti"].xcom_push(key="dim_actions", value=dim_actions.to_json(orient="records"))
    context["ti"].xcom_push(
        key="fact_user_actions",
        value=fact_user_actions.to_json(orient="records", date_format="iso"),
    )
    context["ti"].xcom_push(
        key="rejected_records_df",
        value=rejected_records_df.to_json(orient="records"),
    )
    context["ti"].xcom_push(key="count_summary", value=count_summary)


def quality_check_task(**context):
    dim_users_json = context["ti"].xcom_pull(task_ids="transform_logs", key="dim_users")
    dim_actions_json = context["ti"].xcom_pull(task_ids="transform_logs", key="dim_actions")
    fact_user_actions_json = context["ti"].xcom_pull(task_ids="transform_logs", key="fact_user_actions")
    count_summary = context["ti"].xcom_pull(task_ids="transform_logs", key="count_summary")

    dim_users = pd.read_json(StringIO(dim_users_json))
    dim_actions = pd.read_json(StringIO(dim_actions_json))
    fact_user_actions = pd.read_json(StringIO(fact_user_actions_json))

    checks = ETL_Pipeline.run_quality_checks(
        dim_users,
        dim_actions,
        fact_user_actions,
        count_summary,
    )

    print("Data quality checks passed:", checks)
    context["ti"].xcom_push(key="dq_checks", value=checks)


def load_raw_task(**context):
    raw_data = context["ti"].xcom_pull(task_ids="extract_raw_logs", key="raw_data")
    source_file_name = context["ti"].xcom_pull(task_ids="extract_raw_logs", key="source_file_name")

    conn = None
    try:
        conn = ETL_Pipeline.get_connection()
        with conn:
            ETL_Pipeline.load_raw_to_postgres(conn, raw_data, source_file_name)
    finally:
        if conn:
            conn.close()

    print("Raw data loaded successfully into raw_user_logs")


def load_quarantine_task(**context):
    rejected_records_json = context["ti"].xcom_pull(task_ids="transform_logs", key="rejected_records_df")
    rejected_records_df = pd.read_json(StringIO(rejected_records_json))

    if rejected_records_df.empty:
        print("No rejected records found. Skipping quarantine load.")
        return

    conn = None
    try:
        conn = ETL_Pipeline.get_connection()
        with conn:
            ETL_Pipeline.load_quarantine_to_postgres(conn, rejected_records_df)
    finally:
        if conn:
            conn.close()

    print(f"Rejected records moved to quarantine table: {len(rejected_records_df)}")


def load_curated_task(**context):
    dq_checks = context["ti"].xcom_pull(task_ids="run_quality_checks", key="dq_checks")
    if dq_checks is None:
        raise ValueError("Data quality checks result missing — aborting curated load.")

    dim_users_json = context["ti"].xcom_pull(task_ids="transform_logs", key="dim_users")
    dim_actions_json = context["ti"].xcom_pull(task_ids="transform_logs", key="dim_actions")
    fact_user_actions_json = context["ti"].xcom_pull(task_ids="transform_logs", key="fact_user_actions")

    dim_users = pd.read_json(StringIO(dim_users_json))
    dim_actions = pd.read_json(StringIO(dim_actions_json))
    fact_user_actions = pd.read_json(StringIO(fact_user_actions_json))

    conn = None
    try:
        conn = ETL_Pipeline.get_connection()
        with conn:
            ETL_Pipeline.load_to_postgres(
                conn,
                dim_users,
                dim_actions,
                fact_user_actions,
            )
    finally:
        if conn:
            conn.close()

    print("Curated data loaded successfully into dim/fact tables")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_actions_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for raw logs into PostgreSQL with raw, quarantine, and curated layers",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "postgres", "json", "airflow"],
) as dag:

    extract = PythonOperator(
        task_id="extract_raw_logs",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform_logs",
        python_callable=transform_task,
    )

    quality_check = PythonOperator(
        task_id="run_quality_checks",
        python_callable=quality_check_task,
    )

    load_raw = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_task,
    )

    load_quarantine = PythonOperator(
        task_id="load_quarantine_to_postgres",
        python_callable=load_quarantine_task,
    )

    load_curated = PythonOperator(
        task_id="load_curated_to_postgres",
        python_callable=load_curated_task,
    )

    extract >> transform >> quality_check
    transform >> load_quarantine
    quality_check >> load_curated
    extract >> load_raw