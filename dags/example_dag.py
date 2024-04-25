import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python_operator import (
    BranchPythonOperator,
    PythonOperator,
)
from logging import info

with DAG(
    "example_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:

    def example_dag():
        info("Ok")

    report_dag_slack_task = PythonOperator(
        task_id="example_dag",
        python_callable=example_dag,
    )

    report_dag_slack_task