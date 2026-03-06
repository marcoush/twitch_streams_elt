from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

default_start_date = datetime.now() - timedelta(days=1)

@dag(
    start_date=default_start_date,
    schedule=None,  
    catchup=False,
    tags=["test"]
)
def test_connections_dag():

    @task()
    def check_conn():
        conn = BaseHook.get_connection('snowflaky_conn')  # replace with your actual connection ID
        print("Host:", conn.host)
        print("Login:", conn.login)
        print("Port:", conn.port)
        print("Extra:", conn.extra)

    check_conn()

test_connections_dag = test_connections_dag()