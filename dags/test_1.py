import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="test_1",
    start_date=datetime.datetime(2025, 1, 1),
    schedule="@daily",
):
    EmptyOperator(task_id="task")