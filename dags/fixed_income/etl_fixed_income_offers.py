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
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "etl_fixed_income_offers",
    default_args=default_args,
    description="Load fixed income offers from BankPro",
    schedule_interval="*/30 10-15 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["BankPro", "Fixed_Income", "Offers"],
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
            f"""SELECT  o.seqoferta AS id_offer,
        CASE
            WHEN orc.nomrotinacalculo = 'PRÉ' THEN op.siglapapel + ' Prefixado'
            ELSE op.siglapapel + ' Pós-fixado'
        END AS description_offer,
        o.seqproduto AS product_id,
        CONVERT(DATETIME, o.dtavalidade) AS effective_date,
        o.dtavencto AS expiration_date,
        CONVERT(bit, case when o.staaprovada = 'S' then 'true'  else  'false' end) AS approved,
        Isnull(o.numlotearquivo, '') AS batch_file,
        o.percdistribuicao AS emission_percentage,
        o.percemissao AS distribution_percentage,
        isnull(o.txadistribuicao, 0) AS issuance_fee,
        isnull(o.txaemissao, 0) AS distribution_fee,
        ' ' AS criterion_calculation,
        o.pzovencimento AS due_days,
        o.pzocarencia AS grace_period,
        isnull(dateadd(day, o.PzoCarencia, getdate()), getdate()) AS grace_period_date,
        o.puemissao AS unitary_price,
        o.valminimoaplicacao AS minimum_application_value,
        o.valminimoresgate AS minimum_redemption_value,
        o.valmaximoaplicacao AS maximum_value_application,
        Trim(ofla.desformaliqadm) AS liquidation_form,
        Trim(oc.nomclearing) AS custody_location,
        0 AS issue_price,
        Concat(Substring(CONVERT(VARCHAR, CONVERT(DATETIMEOFFSET, Dateadd(hh, 3, Trim(Stuff(p.horainiciooperacao, 3, 0, ':'))))), 12, 12), ' +0000') AS start_time_of_operation,
        Concat(Substring(CONVERT(VARCHAR, CONVERT(DATETIMEOFFSET, Dateadd(hh, 3, Trim(Stuff(p.horaterminooperacao, 3, 0, ':'))))), 12, 12), ' +0000') AS end_time_of_operation,
        op.siglapapel AS title,
        CASE p.tpopessoa
            WHEN 1 THEN 'Física'
            WHEN 2 THEN 'Jurídica'
            ELSE ''
        END AS person_type,
        CASE p.tpocanal
            WHEN 3 THEN 'App'
            ELSE ''
        END AS channel_type,
        p.nomproduto AS product,
        cg.nomcliente AS issuer,
        CASE orc.nomrotinacalculo
            WHEN 'DI-OVER' THEN 'CDI'
            WHEN 'DI' THEN 'CDI'
            WHEN 'PRÉ' THEN 'PRÉ'
            WHEN 'IPCA #Índice' THEN 'IPCA'
            WHEN 'IPCA Fator' THEN 'IPCA'
            ELSE orc.nomrotinacalculo
        END AS "index",
        CONVERT(bit, case when p.staliquidez = 'S' then 'true'  else  'false' end) AS liquidity,
        case p.StaLiquidez
            when 'S' then 'Diária'
            when 'N' then 'No vencimento'
            else '' 
        end AS description_liquidity,
        CONVERT(bit, case when p.stagarantidofgc = 'S' then 'true'  else  'false' end) AS fgc_guarantee,
        case p.TpoRisco
            when 0 then 'Muito baixo'
            when 1 then 'Baixo' 
            when 2 then 'Medio' 
            when 3 then 'Alto' 
            else '' 
        end AS risk,
        Isnull(p.desobservacao, '') AS description_1,
        Isnull(p.desobservacao2, '') AS description_2,
        CONVERT(bit, case when op.stapagair = 'S' then 'false'  else  'true' end) AS exempt_product_ir,
        CASE orc.nomrotinacalculo
            WHEN 'DI' THEN Cast(CONVERT(DECIMAL(5, 2), o.percemissao) AS VARCHAR) + '% do CDI'
            WHEN 'PRÉ' THEN Cast(CONVERT(DECIMAL(5, 2), o.txaemissao) AS VARCHAR) + '% a.a.'
            WHEN 'IPCA' THEN 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), o.txaemissao) AS VARCHAR) + '% a.a.'
            WHEN 'IPCA #Índice' THEN 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), o.txaemissao) AS VARCHAR) + '% a.a.'
            ELSE ''
        END AS description_profitability,
        'PicPay Invest' AS custodian_name,
        'PicPay Invest' AS broker_name,
        case orc.NomRotinaCalculo 
            when 'PRÉ' then 'Prefixado'
            else 'Pós-fixado' 
        end as type_product_offer,
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
        where o.StaAprovada = 'S'
        and o.DtaValidade = convert(date,getdate())""",
            cnxn,
        )
        logging.info("Finish extraction")
        docs_overwrite = df.to_dict(orient="records")

        return docs_overwrite

    def load_to_mongodb(query_results):
        url = Variable.get("FIXED_INCOME_MONGO_URL")
        database = Variable.get("FIXED_INCOME_MONGO_DATABASE")
        collection = Variable.get("FIXED_INCOME_MONGO_COLLECTION_OFFERS")
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
            send_slack_message_func("Erro ao deletar informações no Mongo", "Exception")
        else:
            logging.info("Collection data has been deleted")
            try:
                col.insert_many(query_results, ordered=False)
            except:
                logging.info("Exception occured adding documents in Mongo")
                send_slack_message_func("Erro ao adicionar informações no Mongo", "Exception")
            else:
                logging.info(
                    f"The data has been inserted into the collection, {len(query_results)} docs"
                )

    def send_slack_message_func(error, type):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - FIXED INCOME - OFFERS* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_fixed_income_offers",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - FIXED INCOME - OFFERS*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_fixed_income_offers",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            
    def main(**context):
        try:
            query_results = extract_data()
            load_to_mongodb(query_results)
            send_slack_message_func("", "")
            return "sucess"
        except Exception as e:
            logging.error("ETL error", e)
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
