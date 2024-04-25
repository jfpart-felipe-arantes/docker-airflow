import logging
from datetime import datetime, timedelta

import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import pymssql
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
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "etl_posicao_fundos",
    default_args=default_args,
    description="Load positions funds into PostgreSQL from PMS",
    schedule_interval="30 7-18 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["PMS", "Funds", "Position"],
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
        server = Variable.get("SQL_SERVER_PMS_SERVER")
        database = Variable.get("SQL_SERVER_PMS_DATABASE")
        username = Variable.get("SQL_SERVER_PMS_USER")
        password = Variable.get("SQL_SERVER_PMS_PASSWORD")

        connection = pymssql.connect(server, username, password, database)
        cursor = connection.cursor(as_dict=True)

        query = f"""
Select
                        RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR,PE.cd_CPFCNPJ),11)    As document
                        ,null                                                    As account
                        ,DEF.id_Fundo                                                   As id_fundo
                        ,PEP.ds_Nome                                                    As nome_fundo
                        ,sum(round(PS.vl_CotaCusto * PS.qt_Cotas, 2))                   As valor_bruto_investido
                        ,sum(round(PS.vl_Bruto - (PS.vl_CotaCusto * PS.qt_Cotas), 2))   As rendimento_bruto
                        ,sum(PS.vl_Bruto)                                               As valor_bruto_atual
                        ,min(dt_Aplicacao)                                              As data_aplicacao
                        ,sum(PS.qt_Cotas)                                               As qtd_atual
                        ,sum(PS.qt_Bloqueada)                                           As qtd_bloqueada
                    From SGI_TRN.dbo.tb_PasSaldo PS (nolock)
                    Join SGI_TRN.dbo.tb_CadCotista C (nolock)
                        On C.id_Cotista = PS.id_Cotista
                    Join SGI_TRN.dbo.tb_CadPortfolio P (nolock)
                        On P.id_Portfolio = PS.id_Portfolio
                    Join SGI_TRN.dbo.tb_CadPessoa PE (nolock)
                        On PE.id_Pessoa = C.id_Pessoa and PE.tp_Pessoa = 'F'
                    Join SGI_TRN.dbo.tb_CadPessoa PEP (nolock)
                        On PEP.id_Pessoa = P.id_Pessoa 
                    Join SGI_TRN.dbo.vw_DetalheFundo DEF
                        On DEF.CNPJ = PEP.cd_CPFCNPJ 
                    Where PS.qt_cotas> '0.001' and vl_bruto != 0
                    Group By PE.cd_CPFCNPJ, DEF.id_Fundo, PEP.ds_Nome
        """
        try:
            cursor.execute(query)
            values = cursor.fetchall()
        except Exception as e:
            logging.error(
                "The extraction query execution encountered an error:", e
            )
            send_slack_message_func("Erro na extração dos dados do SQL Server", "Exception")
            raise AirflowException("The DAG has been marked as failed.")
        finally:
            connection.close()
        df = pd.DataFrame(values)
        return df

    def create_engine_postgresql():
        db_host = Variable.get("POSTGRES_FUNDS_HOST")
        db_name = Variable.get("POSTGRES_FUNDS_NAME")
        db_password = Variable.get("POSTGRES_FUNDS_PASSWORD")
        db_port = Variable.get("POSTGRES_FUNDS_PORT")
        db_user = Variable.get("POSTGRES_FUNDS_USER")

        postgresql_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        conn = create_engine(postgresql_url).connect()

        return conn

    def process_dataframe(df):
        created_date = datetime.utcnow()
        df["created_at"] = created_date
        coluns_posicao_fundos = {
            "account",
            "created_at",
            "data_aplicacao",
            "document",
            "id_fundo",
            "nome_fundo",
            "qtd_atual",
            "qtd_bloqueada",
            "rendimento_bruto",
            "valor_bruto_atual",
            "valor_bruto_investido",
        }
        colunas_comuns = [
            coluna for coluna in df.columns if coluna in coluns_posicao_fundos
        ]
        df_final = df[colunas_comuns]

        return df_final

    def load_to_postgesql(df, conn):
        try:
            df.to_sql(
                "posicao_fundos",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10000,
            )
        except Exception as e:
            logging.error(
                "An error occurred while inserting the DataFrame into the table:",
                e,
            )
            send_slack_message_func("Erro na inserção dos dados no PostGres", "Exception")
            raise AirflowException("The DAG has been marked as failed.")

    def send_slack_message_func(error, type):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - POSIÇÃO FUNDOS* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_posicao_fundos",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - POSIÇÃO FUNDOS*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_posicao_fundos",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            
    def main(**context):
        try:
            df = extract_data()
            processed_df = process_dataframe(df)
            conn_load = create_engine_postgresql()
            load_to_postgesql(processed_df, conn_load)
            conn_load.close()
            send_slack_message_func("", "")
            return "sucess"
        except Exception as e:
            send_slack_message_func(e, "Exception")

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

    end_open = DummyOperator(
        task_id="end_open",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    b3_is_open >> etl_task >> end_open 
