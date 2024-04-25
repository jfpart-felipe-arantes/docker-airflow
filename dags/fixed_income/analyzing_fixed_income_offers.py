import datetime
import logging
from datetime import datetime, timedelta

import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import pyodbc
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
from pymongo import MongoClient

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

default_args = {
    "owner": "fixed_income",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "analyzing_fixed_income_offers",
    default_args=default_args,
    description="Load analyzing_fixed_income_offers from BankPro",
    schedule_interval="*/30 10-15 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["analyzing", "Fixed_Income", "Offers"],
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

    def extract_data():
        servidor_sqlserver = Variable.get("FIXED_INCOME_SQLSERVER_HOST")
        database = Variable.get("FIXED_INCOME_SQLSERVER_DATABASE")
        usuario = Variable.get("FIXED_INCOME_SQLSERVER_USER")
        senha = Variable.get("FIXED_INCOME_SQLSERVER_PASSWORD")
        cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor_sqlserver
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )
        logging.info("Start extraction")
        df = pd.read_sql(
            f"""        
        SELECT  
            o.seqoferta AS offer_id,
            o.seqproduto AS product_id,
            o.dtavencto AS expiration_date,
            o.staaprovada AS approved, --> Retornar S ou N
            o.percdistribuicao AS emission_percentage,
            o.percemissao AS distribution_percentage,
            isnull(o.txadistribuicao, 0) AS emission_fee,
            isnull(o.txaemissao, 0) AS distribution_fee,
            o.pzovencimento AS due_days,
            o.pzocarencia AS grace_period,
            o.puemissao AS unitary_price,
            o.valminimoaplicacao AS minimum_application_value,
            o.valminimoresgate AS minimum_redemption_value,
            o.valmaximoaplicacao AS maximum_application_value,
            Concat(Substring(CONVERT(VARCHAR, CONVERT(DATETIMEOFFSET, Dateadd(hh, 3, Trim(Stuff(p.horainiciooperacao, 3, 0, ':'))))), 12, 12), ' +0000') AS start_time_of_operation,
            Concat(Substring(CONVERT(VARCHAR, CONVERT(DATETIMEOFFSET, Dateadd(hh, 3, Trim(Stuff(p.horaterminooperacao, 3, 0, ':'))))), 12, 12), ' +0000') AS end_time_of_operation,
            op.siglapapel AS security_description,
            p.nomproduto AS product_name,
            cg.nomcliente AS issuer_name, 
            orc.nomrotinacalculo AS "index",
            p.staliquidez AS liquidity, --> Retornar S ou N
            p.stagarantidofgc AS fgc_guarantee, --> Retornar S ou N
            p.TpoRisco AS risk,
            p.desobservacao AS note_1,
            p.desobservacao2 AS note_2,
            op.stapagair AS exempt_product_ir,--> Retornar S ou N
            Getdate() AS created_at
        FROM   [SCMLionXOpen].[dbo].ope_ofertaproduto o
        INNER JOIN [SCMLionXOpen].[dbo].ope_produtoinvestimento p
        ON p.seqproduto = o.seqproduto
        INNER JOIN [SCMLionXOpen].[dbo].ope_papel op
        ON op.codpapel = p.codpapel
        INNER JOIN [SCMLionXOpen].[dbo].ope_rotinacalculo orc
        ON orc.codrotinacalculo = p.codrotinacalculo
        INNER JOIN [SCMLionXOpen].[dbo].ope_formaliqadm ofla
        ON ofla.seqformaliqadm = p.seqformaliqadm
        INNER JOIN [SCMLionXOpen].[dbo].ope_contacliente occ
        ON occ.seqconta = p.seqcontaemitente
        INNER JOIN [SCMLionXOpen].[dbo].cli_geral cg
        ON cg.codcliente = occ.codcliente
        INNER JOIN [SCMLionXOpen].[dbo].ope_clearing oc
        ON oc.seqclearing = p.seqclearing
        where o.StaAprovada = 'S'""",
            cnxn,
        )
        logging.info("Finish extraction")
        docs_overwrite = df.to_dict(orient="records")

        return docs_overwrite

    def load_to_mongodb(query_results):
        url = Variable.get("FIXED_INCOME_MONGO_URL")
        database = Variable.get("FIXED_INCOME_MONGO_DATABASE")
        collection = Variable.get("ANALYZING_FIXED_INCOME_MONGO_COLLECTION_OFFERS")
        client = MongoClient(url)
        db = client[database]
        col = db[collection]
        col.delete_many({})
        col.insert_many(query_results, ordered=False)
        logging.info("Finish Load")

        try:
            col.delete_many({})
        except:
            logging.info("Exception occured deleting documents in Mongo")
        else:
            logging.info("Collection data has been deleted")
            try:
                col.insert_many(query_results, ordered=False)
            except:
                logging.info("Exception occured adding documents in Mongo")
            else:
                logging.info(
                    f"The data has been inserted into the collection, {len(query_results)} docs"
                )

    def report_dag_slack(**context):
        approved = context["task_instance"].xcom_pull(task_ids="etl")
        now = datetime.now(timezone)
        next_execution_date = croniter(str(dag.schedule_interval), now).get_next(
            datetime
        )
        if approved:
            slack_msg = """
:white_check_mark: Sucess:
*Dag*: {dag}
*Execution Time*: {exec_date}
*Next run:* {next_execution_date} :alarm_clock:
""".format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
                next_execution_date=next_execution_date,
            )
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                slack_msg=slack_msg,
            )

    def main(**context):
        now = datetime.now(timezone)
        next_execution_date = croniter(str(dag.schedule_interval), now).get_next(
            datetime
        )
        ti = context["ti"]
        current_attempts = ti.try_number
        try:
            query_results = extract_data()
            load_to_mongodb(query_results)
            return "sucess"
        except Exception as e:
            logging.error("ETL error", e)
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
<@U04V21FNP16> , <@U05LU8M4CDP>
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

    etl_task = PythonOperator(
        task_id="etl",
        python_callable=main,
        provide_context=True,
    )

    report_dag_slack_task = PythonOperator(
        task_id="report_dag_slack",
        python_callable=report_dag_slack,
    )

    end_open = DummyOperator(
        task_id="end_open",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    b3_is_open >> etl_task >> end_open >> report_dag_slack_task
