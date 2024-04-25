import pyodbc
import pandas as pd
import pytz
from pymongo import MongoClient
import psycopg2
from datetime import datetime
from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from logging import info

from public_functions.slack_integration import send_slack_message
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")
timezone = pytz.timezone("America/Sao_Paulo")
default_args = {
    "owner": "posvenda",
}

with DAG(
    "etl_movements_sirsan",
    default_args=default_args,
    description="Load orders into MongoDB from sirsan",
    schedule_interval="0/5 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["Sirsan", "Orders", "Wallet", "Movements"],
) as dag:
    dag.doc_md = __doc__

    def extract_data_postgre():
        db_host = Variable.get("POSTGRES_FUNDS_HOST")
        db_name = Variable.get("POSTGRES_FUNDS_NAME")
        db_password = Variable.get("POSTGRES_FUNDS_PASSWORD")
        db_port = Variable.get("POSTGRES_FUNDS_PORT")
        db_user = Variable.get("POSTGRES_FUNDS_USER")
        connection_postgre = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
        )
        cursor_postgre = connection_postgre.cursor()
        query_postgre = """
                        WITH
                            newestFund AS  (SELECT max(created_at) AS created_at FROM detalhes_fundos)
                        select
                            fo.investment_fund_id   AS idProduct,
                            fo.id					as idOrder,
                            fo.investor_cpfcnpj     AS document,
                            'InvestmentFunds'       AS type,
                            df.nome_fundo           AS assetDescription,
                            NULL as assetIssuer,
                            'AA'                    AS typeLaunch,
                            df.dias_conversao_aplic AS quoteDeadline,
                            null as processingDeadline,
                            CASE
                                WHEN df.tipo_conversao_aplic = 'U' then 'N'
                                ELSE 'S'
                            END                     AS quoteDeadlineDaysType,
                            null as processingDeadlineDaysType,
                            cast(fo.order_amount as float)         AS amount,
                            null as orderDate,
                            null as orderLiqDate,
                            null as orderQuoteDate
                        FROM investment_fund_orders fo
                            INNER JOIN detalhes_fundos df on cast(df.id_ativo_fundos as varchar) = fo.investment_fund_id
                            INNER JOIN newestFund nf on nf.created_at = df.created_at
                        where
                            fo.order_status = 6;
                        """

        cursor_postgre.execute(query_postgre)
        data_extract_postgre = cursor_postgre.fetchall()
        return data_extract_postgre

    def extract_data_sqlserver():
        servidor = Variable.get("SQL_SERVER_PMS_SERVER")
        database = Variable.get("SQL_SERVER_PMS_DATABASE")
        usuario = Variable.get("SQL_SERVER_PMS_USER")
        senha = Variable.get("SQL_SERVER_PMS_PASSWORD")
        connection_bankpro = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )
        cursor_bankpro = connection_bankpro.cursor()
        info('Extracting full')
        query_bankpro = f"""
                        SELECT
                            CAST(b.id_fundo AS varchar) as idProduct,
                            null as idOrder,
                            RIGHT(CONCAT('00000000000', CAST(a.cd_CPFCNPJ AS varchar)), 11) as document,
                            'InvestmentFunds' as "type",
                            b.ds_FundoClube as assetDescription,
                            NULL as assetIssuer,
                            a.tp_Lancamento as typeLaunch,
                            CASE
                                WHEN TRIM(a.tp_Lancamento) = 'R'
                                    THEN a.nr_DiasConvRes
                                ELSE a.nr_DiasConvApl
                            END AS quoteDeadline,
                            CASE
                                WHEN TRIM(a.tp_Lancamento) = 'A'
                                    THEN a.nr_DiasLiqApl
                                ELSE a.nr_DiasLiqRes
                            END AS processingDeadline,
                            CASE
                                WHEN TRIM(a.tp_Lancamento) = 'R'
                                    THEN b.tp_DiasConvRes
                                ELSE b.tp_DiasConvApl
                            END AS quoteDeadlineDaysType,
                            CASE
                                WHEN TRIM(a.tp_Lancamento) = 'R'
                                    THEN b.tp_DiasLiqRes
                                ELSE b.tp_DiasLiqApl
                            END AS processingDeadlineDaysType,
                            vl_Pedido as amount,
                            a.dt_Pedido as orderDate,
                            a.dt_Liquidacao as orderLiqDate,
                            a.dt_Conversao as orderQuoteDate
                        FROM SGI_TRN.dbo.vw_ConsultaMovimentacaoTransito_Picpay as A
                        INNER JOIN SGI_TRN.dbo.vw_DetalheFundo AS b ON b.id_fundoClube = a.id_Portfolio
                        """
        cursor_bankpro.execute(query_bankpro)
        data_extract_bankpro = cursor_bankpro.fetchall()

        info("Values from bankpro are OK")
        info(data_extract_bankpro)

        data_extract_postgre = extract_data_postgre()
        info("Values from postgre query are OK")
        info(data_extract_postgre)
        data_extract_block = []
        data_extract_block.extend(data_extract_bankpro)
        data_extract_block.extend(data_extract_postgre)
        info("End of data extraction")

        return data_extract_block

    def load_data(insert_data: dict, collection):
        try:
            collection.delete_many({})
        except Exception as E:
            info(E)
            send_slack_message_func("Erro ao deletar informações no Mongo", "Exception")
            info("Erro ao deletar informações no Mongo")
        else:
            info("Collection data has been deleted")
            try:
                collection.insert_many(insert_data, ordered=False)
            except Exception as E:
                info(E)
                send_slack_message_func("Erro ao adicionar informações no Mongo", "Exception")
                info("Erro ao adicionar informações no Mongo")
            else:
                info(
                    f"The data has been inserted into the collection, \
                     {len(insert_data)} docs"
                )

    def transform_data(data):
        info("Initializing data transformation")
        insertData = {}
        data_to_insert = []
        for idProduct, idOrder, document, type, assetDescription, assetIssuer, \
            typeLaunch, quoteDeadline, processingDeadline, quoteDeadlineDaysType, \
                processingDeadlineDaysType, amount, orderDate, orderLiqDate, orderQuoteDate in data:
            insertData = ({
                "idProduct": idProduct,
                "idOrder": idOrder,
                "document": document,
                "type": type,
                "assetDescription": assetDescription,
                "assetIssuer": assetIssuer,
                "typeLaunch": typeLaunch,
                "quoteDeadline": quoteDeadline,
                "processingDeadline": processingDeadline,
                "quoteDeadlineDaysType": quoteDeadlineDaysType,
                "processingDeadlineDaysType": processingDeadlineDaysType,
                "amount": amount,
                "orderDate": orderDate,
                "orderLiqDate": orderLiqDate,
                "orderQuoteDate": orderQuoteDate})
            info(insertData)
            data_to_insert.append(insertData)
        info(data_to_insert)
        info("End of data transformation")
        return data_to_insert

    def connect_mongo(string_conn: str, database: str, collection: str):
        client = MongoClient(f"{string_conn}")
        db = client[f"{database}"]
        return db[f"{collection}"]

    def send_slack_message_func(error, type):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - WALLET - ETL MOVEMENTS SIRSAN* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_movements_sirsan",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - WALLET - ETL MOVEMENTS SIRSAN*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_movements_sirsan",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
    
    def main():
        mongo_engine = connect_mongo(
            Variable.get("MONGO_WALLET_MOVEMENTS_URL"),
            Variable.get("MONGO_WALLET_MOVEMENTS_DB"),
            Variable.get("MONGO_WALLET_MOVEMENTS_COL"),
        )

        info("Initializing extraction")
        data = extract_data_sqlserver()
        transformed_data = transform_data(data)
        load_data(transformed_data, mongo_engine)
        send_slack_message_func("", "")

    lambda_task = PythonOperator(task_id="lambda", python_callable=main)