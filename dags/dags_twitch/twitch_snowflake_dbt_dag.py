from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt/dbt_twitch"
DBT_PROFILES_DIR = "/opt/airflow/dbt/dbt_twitch"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="twitch_snowflake_dbt_dag",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["twitch", "dbt", "snowflake"]
) as dag:

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --profiles-dir {DBT_PROFILES_DIR}
        """
    )

    run_dbt