import datetime
import logging
from datetime import datetime, timedelta

import pandas_market_calendars as mcal
import pendulum
import pyodbc
import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
    PythonOperator,
)
from public_functions.slack_integration import send_slack_message

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="ETL Open Finance Funds OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")


local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

default_args = {
    "owner": "open_finance",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "etl_open_finance_funds",
    default_args=default_args,
    description="Load funds from open finance",
    schedule_interval="0 8 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["open_finance", "Funds"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:
    dag.doc_md = __doc__

    def is_b3_open():
        try:
            now = datetime.now(timezone)
            b3_calendar = mcal.get_calendar("B3")
            execution_date = now.date()
            is_b3_open_check = (
                b3_calendar.valid_days(
                    start_date=execution_date, end_date=execution_date
                ).size
                > 0
            )
            if is_b3_open_check:
                print(f"B3 is open today: {execution_date}")
                return "b3_is_open"
            else:
                print(f"B3 is closed today: {execution_date}")
                return "b3_is_closed"
        except Exception as e:
            print(f"Error while checking if B3 is open: {e}")
            return "b3_is_closed"


    def create_connection():
        servidor_sqlserver = Variable.get("FIXED_INCOME_SQLSERVER_HOST")
        database = "SGI_TRN"
        usuario = Variable.get("FIXED_INCOME_SQLSERVER_USER")
        senha = Variable.get("FIXED_INCOME_SQLSERVER_PASSWORD")
        cnxn_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor_sqlserver
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )
        cnxn = pyodbc.connect(cnxn_string)

        return cnxn

    def template_connection(cnxn, source_table, truncate_query, destination_table):
        cursor = cnxn.cursor()

        insert_query = 'INSERT INTO ' + destination_table + ' SELECT * FROM ' + source_table

        cursor.execute(truncate_query)
        cursor.execute(insert_query)

        cnxn.commit()

    def end_connection(cnxn, **kwargs):
        cnxn.close()

    def investments_application(cnxn, **kwargs):
        logging.info("Start extraction vw_openfinance_fundos_investments_application")

        source_table = (
            "SGI_TRN.dbo.vw_openfinance_fundos_investments_application"
        )
        truncate_query = (
            "truncate table SGI_TRN.dbo.tb_openfinance_fundos_investments_application"
        )

        destination_table = "SGI_TRN.dbo.tb_openfinance_fundos_investments_application"

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_fundos_balances")

    def balances(cnxn, **kwargs):
        logging.info("Start extraction vw_openfinance_fundos_balances")

        source_table = (
            "SGI_TRN.dbo.vw_openfinance_fundos_balances"
        )
        truncate_query = (
            "truncate table SGI_TRN.dbo.tb_openfinance_fundos_balances"
        )

        destination_table = "SGI_TRN.dbo.tb_openfinance_fundos_balances"

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_fundos_balances")

    def investments(cnxn, **kwargs):

        logging.info("Start extraction in vw_openfinance_fundos_investments")

        source_table = (
            "SGI_TRN.dbo.vw_openfinance_fundos_investments"
        )

        truncate_query = (
            "truncate table SGI_TRN.dbo.tb_openfinance_fundos_investments"
        )

        destination_table = "SGI_TRN.dbo.tb_openfinance_fundos_investments"

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_fundos_investments")

    def investments_investimentsid(cnxn, **kwargs):
    
        logging.info("Start extraction in vw_openfinance_fundos_investments_investimentsid")

        source_table = "SGI_TRN.dbo.vw_openfinance_fundos_investments_investimentsid"

        truncate_query = "truncate table SGI_TRN.dbo.tb_openfinance_fundos_investments_investimentsid"

        destination_table = (
            "SGI_TRN.dbo.tb_openfinance_fundos_investments_investimentsid"
        )

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_fundos_investments_investimentsid")

    def transactions(cnxn, **kwargs):

        logging.info("Start extraction")

        source_table = (
            "SGI_TRN.dbo.vw_openfinance_fundos_transactions"
        )

        truncate_query = (
            "truncate table SGI_TRN.dbo.tb_openfinance_fundos_transactions"
        )

        destination_table = "SGI_TRN.dbo.tb_openfinance_fundos_transactions"

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_fundos_transactions")

    def transactions_current(cnxn, **kwargs):

        logging.info("Start extraction in vw_openfinance_fundos_transactions_current")

        source_table = "SGI_TRN.dbo.vw_openfinance_fundos_transactions_current"

        truncate_query = "truncate table SGI_TRN.dbo.tb_openfinance_fundos_transactions_current"

        destination_table = (
            "SGI_TRN.dbo.tb_openfinance_fundos_transactions_current"
        )

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_fundos_transactions_current")

    def decide_branch(**context):
        approved = context["task_instance"].xcom_pull(task_ids="is_b3_open")
        if approved:
            return "b3_is_open"
        else:
            return "b3_is_closed"

    is_b3_open_task = BranchPythonOperator(
        task_id="is_b3_open",
        python_callable=is_b3_open,
        op_kwargs={"execution_date": "{{ execution_date }}"},
    )

    b3_is_open = DummyOperator(
        task_id="b3_is_open",
    )

    b3_is_closed = DummyOperator(
        task_id="b3_is_closed",
    )

    cnxn = create_connection()

    end_open = DummyOperator(
        task_id="end_open",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    investments_application_task = PythonOperator(
        task_id="investments_application", python_callable=investments_application, op_kwargs={"cnxn": cnxn}
    )

    balances_task = PythonOperator(
        task_id="balances", python_callable=balances, op_kwargs={"cnxn": cnxn}
    )
    investments_task = PythonOperator(
        task_id="investments", python_callable=investments, op_kwargs={"cnxn": cnxn}
    )
    investments_investimentsid_task = PythonOperator(
        task_id="investments_investimentsid",
        python_callable=investments_investimentsid,
        op_kwargs={"cnxn": cnxn}
    )
    transactions_task = PythonOperator(
        task_id="transactions", python_callable=transactions, op_kwargs={"cnxn": cnxn}
    )
    transactions_current_task = PythonOperator(
        task_id="transactions_current", python_callable=transactions_current, op_kwargs={"cnxn": cnxn}
    )
    end_connection_task = PythonOperator(
        task_id="end_connection", python_callable=end_connection, op_kwargs={"cnxn": cnxn}
    )
    
    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    (
        b3_is_open
        >>investments_application_task
        >> balances_task
        >> investments_task
        >> investments_investimentsid_task
        >> transactions_task
        >> transactions_current_task
        >> end_connection_task
        >> end_open
    )
