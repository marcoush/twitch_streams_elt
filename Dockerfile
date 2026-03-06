FROM apache/airflow:latest

RUN pip install --no-cache-dir dbt-core dbt-snowflake