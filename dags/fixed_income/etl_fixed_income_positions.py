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
from airflow.operators.python import PythonOperator
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
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "etl_fixed_income_positions",
    default_args=default_args,
    description="Load fixed income positions from BankPro",
    schedule_interval="0 3 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["BankPro", "Fixed_Income", "Positions"],
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

    def extract_data():
        servidor_sqlserver = Variable.get("FIXED_INCOME_SQLSERVER_HOST")
        database = Variable.get("FIXED_INCOME_SQLSERVER_DATABASE")
        usuario = Variable.get("FIXED_INCOME_SQLSERVER_USER")
        senha = Variable.get("FIXED_INCOME_SQLSERVER_PASSWORD")
        url = Variable.get("FIXED_INCOME_MONGO_URL")
        database_mongo = Variable.get("FIXED_INCOME_MONGO_DATABASE")
        collection = Variable.get("FIXED_INCOME_MONGO_COLLECTION_POSITIONS")
        client = MongoClient(url)
        db = client[database_mongo]
        col = db[collection]
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
            f"""SELECT distinct a.custodia AS external_id,
            a.idcliente AS customer_id,
            a.clienteintegracao AS account_number,
            CONVERT(INT, a.integracao) AS integration,
            a.nome AS customer_name,
            a.cpfcnpj AS customer_document,
            ISNULL(a.idproduto, 0) AS product_id,
            a.papel AS security_description,
            trim(a.titulo) AS security_id,
            CASE
            WHEN a.indexador = 'PRÉ' THEN 'Prefixado'
            ELSE 'Pós-fixado'
            END AS position_product_type,
            CASE
                WHEN a.indexador = 'PRÉ' OR a.indexador is null THEN a.Papel + ' PREFIXADO'
                ELSE a.Papel + ' PÓS-FIXADO'
            END  AS position_description,
            a.Papel + ' Turbo' AS position_escalation_description,
            isnull(o.ValMinimoResgate, 105) as minimum_redemption,
            ISNULL(convert(float,a.taxajuros), 0) AS tax,
            Getdate() AS balance_date,
            a.indexador AS indexer,
            ISNULL(convert(float,a.percentualcorrecao), 0) AS indexer_percent,
            CASE a.indexador
                WHEN 'DI' THEN Cast(CONVERT(DECIMAL(5, 2), a.percentualcorrecao) AS VARCHAR) + '% do CDI'
                WHEN 'SELIC' THEN Cast(CONVERT(DECIMAL(5, 2), a.percentualcorrecao) AS VARCHAR) + '% da SELIC'
                WHEN 'PRÉ' THEN Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.'
                WHEN 'IPCA' THEN             
		            CASE
			            when Cast(CONVERT(DECIMAL(5, 2),a.taxajuros)AS VARCHAR) is not null then 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.'
						else Cast(CONVERT(DECIMAL(5, 2), a.percentualcorrecao) AS VARCHAR) + '% do IPCA'
		            END
                ELSE Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.' -- Ver por que vem nulo
            END AS rentability_description,
            a.emissor AS issuer_name,
            CONVERT(DATETIME, a.aplicacao) AS application_date,
            convert(DATETIME,a.vencimento) AS due_date,
            convert(float,a.valorprincipal) AS application_amount,
            convert(float,a.puaplicacao) AS application_unitary_price,
            convert(float,a.quantidade) AS quantity,
            convert(float,a.qtdbloqueada) AS blocked_quantity,
            isnull(b.codmotivobloqueio, 0) AS blocked_id,
            CASE b.codmotivobloqueio
            WHEN 1 THEN 'Bloqueio Judicial'
            WHEN 2 THEN 'Fraude'
            WHEN 3 THEN 'Bloqueio Judicial Original'
            WHEN 4 THEN 'Bloqueio Menor Original'
            WHEN 5 THEN 'Bloqueio Gravame Original'
            WHEN 6 THEN 'Bloqueio Garantia Margem BMF Original'
            WHEN 7 THEN 'Bloqueio Garantia CETIP Original'
            WHEN 8 THEN 'Bloqueio Garantia SELIC Original'
            WHEN 9 THEN 'Bloqueio Garantia OP. INT. Original'
            WHEN 10 THEN 'Bloqueio Garantia Cartao Est. Original'
            WHEN 11 THEN 'Bloqueio Margem Bovespa Original'
            ELSE ''
            END AS blocked_description,
            isnull(convert(float,a.quantidadedisponivel),0) AS available_quantity,
            isnull(convert(float,a.ValorAtual),0) AS gross_total_amount,
            isnull(convert(float,a.ValorAtualDisponivel),0) AS gross_amount,
            isnull(convert(float,a.ValorRendtoBruto),0) AS rentability_amount,
            isnull(convert(float,a.valorir),0) AS ir_amount,
            isnull(convert(float,valoriof),0) AS iof_amount,
            isnull(convert(float,a.valorliquido),0) AS net_amount,
            isnull(convert(float,a.puatual),0) AS current_unitary_price,
            CONVERT(bit, 
            CASE WHEN convert(date,a.Carencia) is not null THEN 
            	case when convert(date,a.Carencia) < convert(date,a.vencimento) THEN
            		case when opi.StaLiquidez is null then 'true'
            			 when opi.StaLiquidez = 'S' then 'true' else 'false' end
            	else 'false' end
	        else 'false'
	        END) AS daily_liquidity,
                        CASE WHEN convert(date,a.Carencia) is not null THEN 
                case when convert(date,a.Carencia) < convert(date,a.vencimento) THEN
                    case when opi.StaLiquidez is null then 'Diária'
                         when opi.StaLiquidez = 'S' then 'Diária' else 'No vencimento' end
                else 'No vencimento' end
            else 'No vencimento'
            END AS liquidity_description,                                                                                                                                                                                                                                                                                                                                                         
            CASE
                WHEN convert(date,a.Carencia) is null then isnull(o.PzoCarencia, 0)
                else DATEDIFF(DAY, convert(date,a.aplicacao), CONVERT(DATETIME, a.Carencia))
            end as grace_period,
            CASE 
                when convert(date,a.Carencia) is null then isnull(dateadd(DAY, o.PzoCarencia, CONVERT(DATETIME, a.aplicacao)), getdate())
                else convert(date,a.Carencia)
            END AS grace_date,
            fl_turbo as is_escalation, 
            Getdate() AS created_at
            FROM [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,convert(date,getdate()),null) a
            LEFT JOIN [SCMLionXOpen].[dbo].ope_produtoinvestimento opi ON opi.seqproduto = a.idproduto
            LEFT JOIN
            (SELECT seqoperacao,
            codmotivobloqueio,
            seqtitulo
            FROM [SCMLionXOpen].[dbo].vw_ope_saldooperacoesbloqueadas
            WHERE qtdbloqueada > 0 GROUP  BY seqoperacao, codmotivobloqueio, seqtitulo) b ON b.seqoperacao = a.custodia AND b.seqtitulo = a.idtitulo
            LEFT JOIN [SCMLionXOpen].[dbo].[ope_motivobloqueio] c ON c.codmotivobloqueio = b.codmotivobloqueio
            LEFT JOIN [SCMLionXOpen].[dbo].OPE_OfertaProduto o ON o.SeqProduto = a.idProduto
            inner join (select nTitloRendaFixa, case when count (1) > 1 then 'true' else 'false' end fl_turbo from SCMLionXPricing.[dbo].tRemunTitloRendaFixa  group by nTitloRendaFixa) t on t.nTitloRendaFixa = a.idtitulo
            WHERE a.custodia IS NOT NULL""",
            cnxn,
        )

        df["daily_liquidity"] = df["daily_liquidity"].astype("bool")
        docs_insert = df.to_dict(orient="records")

        logging.info("Start Steps Overwrite and Load")
        overwrite_to_mongodb(docs_insert)
        load_to_mongodb(docs_insert)

        logging.info("Read DF2")
        df2 = pd.read_sql(
            """SELECT distinct a.custodia AS external_id,
                            convert(datetime,u.dVgciaConfg) as escalation_date, 
                            CASE a.indexador
                                WHEN 'DI' THEN Cast(CONVERT(DECIMAL(5, 2), u.pIndcdRemun) AS VARCHAR) + '% do CDI'
                                WHEN 'PRÉ' THEN Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.'
                                WHEN 'IPCA' THEN 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.'
                                ELSE ''''
                            END AS escalation_description     
                            FROM [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,convert(date,getdate()),null) a
                            inner join SCMLionXPricing.[dbo].tRemunTitloRendaFixa u on u.nTitloRendaFixa = a.idtitulo
                            WHERE a.custodia IS NOT NULL""",
            cnxn,
        )

        insertData = {}
        for key, value in df2.iterrows():
            if value["external_id"] not in insertData:
                insertData[value["external_id"]] = {}
                insertData[value["external_id"]]["external_id"] = (
                    value["external_id"],
                )
                insertData[value["external_id"]]["escalation_data"] = []
            insertData[value["external_id"]]["escalation_data"].append(
                {
                    "escalation_date": value["escalation_date"],
                    "escalation_description": value["escalation_description"],
                }
            )
        logging.info("Start Update in Collection")
        for query_results in insertData.values():
            query = {"external_id": query_results.get("external_id")[0]}
            new_assets = {"escalation_data": query_results.get("escalation_data")}
            col.update_one(query, {"$set": new_assets})

    def overwrite_to_mongodb(query_results):
        url = Variable.get("FIXED_INCOME_MONGO_URL")
        database = Variable.get("FIXED_INCOME_MONGO_DATABASE")
        collection = Variable.get("FIXED_INCOME_MONGO_COLLECTION_POSITIONS")
        client = MongoClient(url)
        db = client[database]
        col = db[collection]

        try:
            col.delete_many({})
        except:
            logging.info("Exception occured deleting documents in Mongo")
            send_slack_message_func("Erro na hora de deletar dados no Mongo", "Exception")
        else:
            logging.info("Collection data has been deleted")
            try:
                col.insert_many(query_results, ordered=False)
            except:
                logging.info("Exception occured adding documents in Mongo")
                send_slack_message_func("Erro na hora de inserir dados no Mongo", "Exception")
            else:
                logging.info(
                    f"The data has been inserted into the collection, {len(query_results)} docs"
                )

    def load_to_mongodb(query_results):
        url = Variable.get("FIXED_INCOME_MONGO_URL")
        database = Variable.get("FIXED_INCOME_MONGO_DATABASE")
        collection = Variable.get("FIXED_INCOME_MONGO_COLLECTION_HISTORIC_POSITIONS")
        client = MongoClient(url)
        db = client[database]
        col = db[collection]

        try:
            col.insert_many(query_results, ordered=False)
        except:
            send_slack_message_func("Erro na hora de inserir dados no Mongo", "Exception")
            logging.info("Exception occured adding documents in Mongo")
        else:
            logging.info(
                f"The data has been inserted into the collection, {len(query_results)} docs"
            )

    def send_slack_message_func(error, type):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - FIXED INCOME POSITIONS* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_fixed_income_positions",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - FIXED INCOME POSITIONS*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_fixed_income_positions",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            

    def main(**context):
        try:
            extract_data()
            send_slack_message_func("", "")
            return "sucess"
        except Exception as e:
            send_slack_message_func(e, "Exception")

    branching_return_null = BranchPythonOperator(
        task_id="branching_return_null", python_callable=check_null_values
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

    send_slack_message_task = PythonOperator(
        task_id="send_slack_message_task",
        python_callable=send_slack_message_func,
        op_kwargs={'error': 'QUERY: "select distinct a.cpfcnpj from [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,convert(date, getdate()),null) a where ValorAtualDisponivel is null" RETORNANDO VALORES', 'type': 'Exception'},
    )

    branching_return_null >> [is_b3_open_task, send_slack_message_task]
    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    b3_is_open >> etl_task >> end_open