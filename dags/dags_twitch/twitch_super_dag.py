from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="super_twitch_dag",
    start_date=datetime(2026, 3, 6),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["twitch", "orchestration", "super"],
) as dag:

    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingest",
        trigger_dag_id="twitch_api_snowflake_ingest_dag",
        wait_for_completion=True,  # Wait until ingestion DAG finishes successfully
        poke_interval=30,
        reset_dag_run=True  # optional, ensures fresh DAG run each time
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="twitch_snowflake_dbt_dag",
        wait_for_completion=False  # dbt DAG runs independently after ingest finishes
    )

    # enforce order
    trigger_ingest >> trigger_dbt