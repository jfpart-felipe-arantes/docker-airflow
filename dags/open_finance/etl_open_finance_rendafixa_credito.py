import datetime
import logging
from datetime import datetime, timedelta

import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import pyodbc
import pytz
from airflow import DAG, AirflowException
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.utils.dates import croniter
from pymongo import MongoClient
from public_functions.slack_integration import send_slack_message


webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")
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
    "open_finance_rendafixa_credito",
    default_args=default_args,
    description="Load fixed income CREDIT from open finance",
    schedule_interval="0 4 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["open_finance", "Fixed_Income", "bankpro"],
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

    def check_null_values():

        servidor = Variable.get("FIXED_INCOME_SQLSERVER_HOST")
        database = Variable.get("FIXED_INCOME_SQLSERVER_DATABASE")
        usuario = Variable.get("FIXED_INCOME_SQLSERVER_USER")
        senha = Variable.get("FIXED_INCOME_SQLSERVER_PASSWORD")

        logging.info("Initializing data validation")

        logging.info("Opening connection at server: " + servidor)

        conn_sql = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )

        df = pd.read_sql(
        "select distinct a.cpfcnpj from [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,convert(date, getdate()),null) a where ValorAtualDisponivel is null",
            conn_sql,
        )
        logging.info(df.head())
        if len(df) > 0:
            return "send_slack_message_task"
        else:
            return "is_b3_open"

    def create_connection():
        servidor_sqlserver = Variable.get("FIXED_INCOME_SQLSERVER_HOST")
        database = Variable.get("FIXED_INCOME_SQLSERVER_DATABASE")
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
        logging.info("Start extraction SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_investments_application")

        source_table = "SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_investments_application"

        truncate_query = "truncate table SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_investments_application"

        destination_table = "SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_investments_application"

        template_connection(cnxn, source_table, truncate_query, destination_table)  

        logging.info("End of insertion in tb_openfinance_rendafixacredito_investments_application")

    def balances(cnxn, **kwargs):
        
        logging.info("Start extraction SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_balances")

        source_table = (
            "SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_balances"
        )

        truncate_query = (
            "truncate table SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_balances"
        )

        destination_table = (
            "SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_balances"
        )

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_rendafixacredito_balances")

    def investments(cnxn, **kwargs):

        logging.info("Start extraction SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_investments")

        source_table = "SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_investments"

        truncate_query = "truncate table SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_investments"

        destination_table = (
            "SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_investments"
        )

        template_connection(cnxn, source_table, truncate_query, destination_table)

        logging.info("End of insertion in tb_openfinance_rendafixacredito_investments")


    def investments_investmentid(cnxn, **kwargs):
        logging.info("Start extraction SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_investments_investmentid")

        source_table = "SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_investments_investmentid"

        truncate_query = "truncate table SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_investments_investmentid"

        destination_table = "SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_investments_investmentid"

        template_connection(cnxn, source_table, truncate_query, destination_table)  

        logging.info("End of insertion in tb_openfinance_rendafixacredito_investments_investmentid")

    def transactions():

        cnxn = create_connection()
        logging.info("Start extraction SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_transactions")

        source_table = "SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_transactions"

        truncate_query = "truncate table SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_transactions"

        destination_table = (
            "SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_transactions"
        )

        template_connection(cnxn, source_table, truncate_query, destination_table) 

        logging.info("End of insertion in tb_openfinance_rendafixacredito_transactions")

    def transactions_current():
        cnxn = create_connection()
        logging.info("Start extraction SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_transactions_current")

        source_table = "SCMLionXOpen.dbo.vw_openfinance_rendafixacredito_transactions_current"

        truncate_query = "truncate table SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_transactions_current"

        destination_table = (
            "SCMLionXOpen.dbo.tb_openfinance_rendafixacredito_transactions_current"
        )

        template_connection(cnxn, source_table, truncate_query, destination_table) 

        logging.info("End of insertion in tb_openfinance_rendafixacredito_transactions_current")

    def send_slack_message_func():

        slack_msg = """
<@U04V21FNP16>
:alert: *ERROR - OPEN FINANCE - RENDA FIXA CREDITO* :alert:
*Dag*: {dag}
*Error*: {error}
*Execution Time*: {exec_date}
            """.format(
                    dag="open_finance_rendafixa_credito",
                    error="Null values in first query",
                    exec_date=datetime.now(timezone)
                )
        send_slack_message(webhook_url_fundos,
                    webhook_url_engineer,
                    slack_msg=slack_msg)
        raise AirflowException("The DAG has been marked as failed.")

    branching_return_null = BranchPythonOperator(
        task_id='branching_return_null',
        python_callable=check_null_values
    )

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


    end_open = DummyOperator(
        task_id="end_open",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    cnxn = create_connection()

    investments_application_task = PythonOperator(
        task_id="investments_application", python_callable=investments_application, op_kwargs={"cnxn": cnxn}
    )

    balances_task = PythonOperator(
        task_id="balances", python_callable=balances, op_kwargs={"cnxn": cnxn}
    )
    investments_task = PythonOperator(
        task_id="investments", python_callable=investments, op_kwargs={"cnxn": cnxn}
    )
    investments_investmentid_task = PythonOperator(
        task_id="investments_investmentid",
        python_callable=investments_investmentid,
        op_kwargs={"cnxn": cnxn}
    )
    transactions_task = PythonOperator(
        task_id="transactions", python_callable=transactions
    )
    transactions_current_task = PythonOperator(
        task_id="transactions_current", python_callable=transactions_current
    )
    end_connection_task = PythonOperator(
        task_id="end_connection", python_callable=end_connection, op_kwargs={"cnxn": cnxn}
    )
    send_slack_message_task = PythonOperator(
        task_id='send_slack_message_task',
        python_callable=send_slack_message_func,
    )

    branching_return_null >> [is_b3_open_task, send_slack_message_task]
    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    (
        b3_is_open
        >> investments_application_task
        >> balances_task
        >> investments_task
        >> investments_investmentid_task
        >> transactions_task
        >> transactions_current_task
        >> end_connection_task
        >> end_open
    )
