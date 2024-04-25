import mysql.connector
import pandas as pd
import pyodbc
import pytz
import pendulum
import logging
from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from logging import info
from airflow.utils.dates import croniter
from public_functions.slack_integration import send_slack_message



conn = mysql.connector.connect(
    host = Variable.get("MYSQL_INVESTPRO_HOST"),
    user = Variable.get("MYSQL_INVESTPRO_USER"),
    password = Variable.get("MYSQL_INVESTPRO_PASSWORD")
)

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

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

UPDATE_OFFER_STATUS_ID = Variable.get("UPDATE_OFFER_STATUS_ID")
SELECT_OFFER_STATUS_ID = Variable.get("SELECT_OFFER_STATUS_ID")

default_args = {
    'owner': 'prevenda',
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}


with DAG(
    "investplace_offers",
    default_args=default_args,
    description="Load offers in mysql of PIT",
    schedule_interval= '0 10 * * 1-5',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["offers", "investplace", "pit"],
    max_active_runs=1,
) as dag:

    dag.doc_md = __doc__

    def update_data():
        cursor = conn.cursor()
        
        update_query = f"""
                        UPDATE offers.offers
                        SET offer_status_id = {UPDATE_OFFER_STATUS_ID}, updated_at = now()
                        WHERE product_type_id in (2,3,4) and offer_status_id = {SELECT_OFFER_STATUS_ID}
                        """

        cursor.execute(update_query)
        conn.commit()
        logging.info("Inserido com sucesso")
        cursor.close()


    def extract_data():
        df = pd.read_sql(
            f"""        
        SELECT 	case
                    when op.siglapapel = 'CDB' then 2
                    when op.siglapapel = 'LCI' then 3
                    when op.siglapapel = 'LCA' then 4 
                end as product_type_id, 
                op.siglapapel as asset_symbol,
                p.nomproduto as name, 
                CASE
                    WHEN orc.nomrotinacalculo = 'PRÉ' THEN op.siglapapel + ' Prefixado'
                ELSE op.siglapapel + ' Pós-fixado'
                END AS description,
                null as image_icon,
                case StaAprovada 
                    when 's' then {SELECT_OFFER_STATUS_ID}
                end as offer_status_id,
                1 as allow_value_order,
                o.valminimoaplicacao as min_value_order,
                o.puemissao as value_multiple_order,
                o.valmaximoaplicacao as max_value_order,
                --null as total_value_amount,
                --null as min_value_to_confirm,
                o.valmaximodistribuicao as value_amount_available, 
                --null as value_amount_reserved,
                --null as value_amount_hired,
                0 as allow_quota_order,
                --null as min_quota_amount_order,
                --null as quota_multiple_order,
                --null as max_quota_amount_order,
                --null as total_quota_amount,
                --null as min_quota_to_confirm,
                --null as quota_amount_available,
                --null as quota_amount_reserved,
                --null as quota_amount_hired,
                CONVERT(DATETIME, o.dtavalidade) as start_date, 
                o.dtavencto as end_date, 
                o.seqoferta as external_code,
                Getdate() as created_at,
                Getdate() as updated_at,
                214 as updated_by
        FROM [SCMLionXOpen].[dbo].ope_ofertaproduto o
        INNER JOIN   [SCMLionXOpen].[dbo].ope_produtoinvestimento p
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
        where o.StaAprovada = 'S'
        and o.DtaValidade = convert(date,getdate())""",
            cnxn,
        )
        logging.info("Extract complete")
        cursor = conn.cursor()

        # Creating a dict of dtypes
        dicionario_dtypes = df.dtypes.to_dict()

        # Replacing their values to datatypes of the oracle database
        repl = {"NA": None,"|O": "VARCHAR2(4000)", "<f8": "NUMBER", "<i8": "VARCHAR2(4000)"}
        dicionario_dtypes = {key: repl.get(value.str, value.str) for key, value in dicionario_dtypes.items()}
        str_interrogacao = ""
        for i in range(0, len(df.columns)):
            str_interrogacao += "?,"
        str_interrogacao = str_interrogacao.rstrip(str_interrogacao[-1])
        lista_colunas = df.columns.tolist()
        lista_colunas = ",".join(str(element) for element in lista_colunas)

        for index, row in df.iterrows():
            try:
                str_tuple = tuple(row)
                tuple_string = ",".join(f"'{elem}'" for elem in str_tuple)
                # Add parentheses to create the desired string format
                tuple_string = f"({tuple_string})"
                insert_sql = (
                    "INSERT INTO "
                    + "offers.offers "
                    + "("
                    + lista_colunas
                    + ") "
                    + "VALUES "
                    + tuple_string
                )
                cursor.execute(insert_sql)
                logging.info("Inserido com sucesso")
                conn.commit()
            except Exception as e:
                logging.info("Erro no seguinte insert")
                logging.info(insert_sql)
                logging.info(e)

    def send_slack_message_func(error, type):
            if type == "Exception":
                slack_msg = """
        <@U04V21FNP16> , <@U05LU8M4CDP>
        :alert: *ERROR - INVESTPLACE OFFERS* :alert:
        *Dag*: {dag}
        *Error*: {error}
        *Execution Time*: {exec_date}
                    """.format(
                            dag="investplace_offers",
                            error=error,
                            exec_date=datetime.now(timezone)
                        )
                send_slack_message(webhook_url_fundos,
                            webhook_url_engineer,
                            slack_msg=slack_msg)
                raise AirflowException("The DAG has been marked as failed.")
            else:
                slack_msg = """
        :white_check_mark: *SUCCESS - INVESTPLACE OFFERS*
        *Dag*: {dag}
        *Execution Time*: {exec_date}
                    """.format(
                            dag="investplace_offers",
                            exec_date=datetime.now(timezone)
                        )
                send_slack_message(webhook_url_fundos,
                            webhook_url_engineer,
                            slack_msg=slack_msg)
            
    def main(**context):
        try:
            update_data()
            extract_data()
            send_slack_message_func("", "")
            return "sucess"
        except Exception as e:
            logging.error("ETL error", e)
            send_slack_message_func(e, "Exception")

    lambda_task = PythonOperator(
        task_id='lambda_task',
        python_callable=main,
    )

    end_task = DummyOperator(
        task_id="end_closed",
    )

    lambda_task >> end_task
