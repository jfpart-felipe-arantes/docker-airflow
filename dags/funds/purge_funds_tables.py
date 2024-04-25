import logging
from datetime import datetime, timedelta

import pandas as pd
import pendulum
import pytz
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.utils.dates import croniter
from public_functions.slack_integration import send_slack_message
from sqlalchemy import create_engine

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

default_args = {
    "owner": "funds",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "purge_details_positions_funds",
    default_args=default_args,
    description="Purge rows from details and positions funds in PostgreSQL",
    schedule_interval="30 19 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["Purge", "Funds", "Position", "Details"],
) as dag:
    dag.doc_md = __doc__


    def create_engine_postgresql():
        db_host = Variable.get("POSTGRES_FUNDS_HOST")
        db_name = Variable.get("POSTGRES_FUNDS_NAME")
        db_password = Variable.get("POSTGRES_FUNDS_PASSWORD")
        db_port = Variable.get("POSTGRES_FUNDS_PORT")
        db_user = Variable.get("POSTGRES_FUNDS_USER")

        postgresql_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        conn = create_engine(postgresql_url).connect()

        return conn

    def purge_details(conn):
        drop_query_details = """
        select purge_detalhes_fundos();
        """

        res_details = conn.execute(drop_query_details)
        logging.info([dict(row) for row in res_details])

    def purge_positions(conn):
        drop_query_positions = """
        select purge_investment_fund_operations();
        """

        res_positions = conn.execute(drop_query_positions)
        logging.info([dict(row) for row in res_positions])

    def main_purge_details(**context):
        now = datetime.now(timezone)
        next_execution_date = croniter(
            str(dag.schedule_interval), now
        ).get_next(datetime)
        ti = context["ti"]
        current_attempts = ti.try_number
        try:
            conn_load = create_engine_postgresql()
            purge_details(conn_load)
            conn_load.close()
            return "success"
        except Exception as e:
            logging.error("Purge error", e)
            if current_attempts <= 2:
                slack_msg = """
:red_circle: Failed {current_attempts} of 3:
*Task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
            """.format(
                    task=context.get("task_instance").task_id,
                    dag=context.get("task_instance").dag_id,
                    exec_date=now,
                    current_attempts=current_attempts,
                )
                send_slack_message(
                    webhook_url_fundos,
                    webhook_url_engineer,
                    slack_msg=slack_msg,
                )
                raise AirflowException("The DAG has been marked as failed.")
            else:
                slack_msg = """
:alert::alert::alert: Failed 3 of 3 :alert::alert::alert:
*Task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
*Next run:* {next_execution_date} :alarm_clock:
            """.format(
                    task=context.get("task_instance").task_id,
                    dag=context.get("task_instance").dag_id,
                    exec_date=now,
                    next_execution_date=next_execution_date,
                )
                send_slack_message(
                    webhook_url_fundos,
                    webhook_url_engineer,
                    slack_msg=slack_msg,
                )
                raise AirflowException("The DAG has been marked as failed.")


    def main_purge_positions(**context):
        now = datetime.now(timezone)
        next_execution_date = croniter(
            str(dag.schedule_interval), now
        ).get_next(datetime)
        ti = context["ti"]
        current_attempts = ti.try_number
        try:
            conn_load = create_engine_postgresql()
            purge_positions(conn_load)
            conn_load.close()
            return "success"
        except Exception as e:
            logging.error("Purge error", e)
            if current_attempts <= 2:
                slack_msg = """
:red_circle: Failed {current_attempts} of 3:
*Task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
            """.format(
                    task=context.get("task_instance").task_id,
                    dag=context.get("task_instance").dag_id,
                    exec_date=now,
                    current_attempts=current_attempts,
                )
                send_slack_message(
                    webhook_url_fundos,
                    webhook_url_engineer,
                    slack_msg=slack_msg,
                )
                raise AirflowException("The DAG has been marked as failed.")
            else:
                slack_msg = """
:alert::alert::alert: Failed 3 of 3 :alert::alert::alert:
*Task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
*Next run:* {next_execution_date} :alarm_clock:
            """.format(
                    task=context.get("task_instance").task_id,
                    dag=context.get("task_instance").dag_id,
                    exec_date=now,
                    next_execution_date=next_execution_date,
                )
                send_slack_message(
                    webhook_url_fundos,
                    webhook_url_engineer,
                    slack_msg=slack_msg,
                )
                raise AirflowException("The DAG has been marked as failed.")
            

    purge_details_task = PythonOperator(
        task_id="purge_details_task",
        python_callable=main_purge_details,
        provide_context=True,
    )

    purge_positions_task = PythonOperator(
        task_id="purge_positions_task",
        python_callable=main_purge_positions,
        provide_context=True,
    )

    purge_details_task >> purge_positions_task