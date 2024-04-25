import pyodbc
import pandas as pd
import pytz
import pendulum
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG, AirflowException
from logging import info

from public_functions.slack_integration import send_slack_message
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")
timezone = pytz.timezone("America/Sao_Paulo")
default_args = {
    "owner": "squad_wallet_and_cash",
}

timezone = pytz.timezone("America/Sao_Paulo")
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "posvenda",
}


with DAG(
    "etl_movements_bankpro",
    default_args=default_args,
    description="Load orders into MongoDB from Bankpro",
    schedule_interval="0/5 10-16 * * 1-5",
    start_date=pendulum.datetime(2023, 8, 14, tz=local_tz),
    catchup=False,
    tags=["Bankpro", "Orders", "Wallet", "Movements"],
) as dag:
    dag.doc_md = __doc__

    def extract_data():
        servidor = Variable.get("SQL_SERVER_PMS_SERVER")
        database = Variable.get("SQL_SERVER_PMS_DATABASE")
        usuario = Variable.get("SQL_SERVER_PMS_USER")
        senha = Variable.get("SQL_SERVER_PMS_PASSWORD")
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

        query = """select
                            g.seqproduto as idProduct,
                            c.id as idBillet,
                            a.NumCPF as document,
                            'FixedIncome' as "type",
                            d.NomCliente as name,
                            c.valboleta as amount,
                            CASE
                                WHEN h.NomRotinaCalculo = 'PRÉ' or h.NomRotinaCalculo is null THEN e.SiglaPapel + ' PREFIXADO'
                                ELSE e.SiglaPapel + ' PÓS-FIXADO'
                            END as assetDescription,
                            NULL as assetIssuer,
                            case
                                when f.iAbrevTpoOperRendaFixa in ('V','Depósito Custod') then 'A'
                                when f.iAbrevTpoOperRendaFixa in ('C','Resg.Venc','Retirada Custod') then 'R'
                            end as typeLaunch,
                            b.dtaliquidacao as liquidityDate,
                            c.valboleta as amount,
                            'Bater' as status,
                            c.DtaBoleta as emissionDate,
                            CASE h.nomrotinacalculo
                                WHEN 'DI' THEN Cast(CONVERT(DECIMAL(5, 2), i.percemissao) AS VARCHAR) + '% do CDI'
                                WHEN 'PRÉ' THEN Cast(CONVERT(DECIMAL(5, 2), i.txaemissao) AS VARCHAR) + '% a.a.'
                                WHEN 'IPCA' THEN 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), i.txaemissao) AS VARCHAR) + '% a.a.'
                                WHEN 'IPCA #Índice' THEN 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), i.txaemissao) AS VARCHAR) + '% a.a.'
                                ELSE ''
                            END AS descriptionProfitability,
                            k.nomcliente as issuer,
                            case g.StaLiquidez
                                when 'N' then 'No vencimento'
                                else 'Diária'
                                    end as liquidity,
                                    j.Valor as positionValue , 
                                    j.ValLiquido as liquidityValue
                        from SCMLionXOpen.dbo.blt_boletaopen b
                        inner join SCMLionXOpen.dbo.blt_boleta c
                            on b.id = c.Id
                        inner join SCMLionXOpen.dbo.CLI_PFisica a
                            on a.Id = c.ClienteId
                        left join SCMLionXOpen.dbo.OPE_Titulo t
                            on b.SeqTitulo = t.SeqTitulo
                        left join [SCMLionXOpen].[dbo].ope_produtoinvestimento g
                            on b.CodPapel = g.CodPapel and (b.SeqProduto = g.SeqProduto or t.SeqProduto = g.SeqProduto)
                        left JOIN [SCMLionXOpen].[dbo].ope_contacliente occ
                            ON occ.seqconta = g.seqcontaemitente   
                        left JOIN [SCMLionXOpen].[dbo].cli_geral k
                            ON k.codcliente = occ.seqconta
                        inner join SCMLionXOpen.dbo.CLI_Geral d
                            on d.Id = c.ClienteId
                        inner join SCMLionXOpen.dbo.OPE_Papel e
                            on b.CodPapel = e.CodPapel
                        inner join SCMLionXOpen.dbo.tTpoOperRendaFixa f
                            on c.TpoOperacao = f.TpoOperacao
                        left join [SCMLionXOpen].[dbo].ope_rotinacalculo h
                            on h.codrotinacalculo = g.codrotinacalculo
                        left join [SCMLionXOpen].[dbo].ope_ofertaproduto i
                            on i.seqproduto = g.seqproduto
                        left join SCMLionXOpen.dbo.vw_oci_fu_ope_operacoeslegado j
                            on j.IdTitulo  = b.SeqTitulo
                        where c.tpoestadoboleta = 'R' and
                        cast(b.dtaliquidacao as date) = convert(date,getdate()) 
                        and b.seqformaliqadm = 3
                """
        try:
            df = pd.read_sql(query, conn_sql)
            data_extract = df.to_dict(orient="records")
            info("Finish extraction")
        except:
            info("An exception occurred while extracting data from SQL")
        finally:
            conn_sql.close()

        return data_extract
    
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

    def connect_mongo(string_conn: str, database: str, collection: str):
        client = MongoClient(f"{string_conn}")
        db = client[f"{database}"]
        return db[f"{collection}"]

    def send_slack_message_func(error, type):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - WALLET - ETL MOVEMENTS BANKPRO* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_movements_bankpro",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - WALLET - ETL MOVEMENTS BANKPRO*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_movements_bankpro",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
    def main():
        mongo_engine = connect_mongo(
            Variable.get("MONGO_WALLET_MOVEMENTS_BANKPRO_URL"),
            Variable.get("MONGO_WALLET_MOVEMENTS_BANKPRO_DB"),
            Variable.get("MONGO_WALLET_MOVEMENTS_BANKPRO_COL"),
        )
        info("Initializing extraction")
        data_from_pms = extract_data()
        load_data(data_from_pms, mongo_engine)

    lambda_task = PythonOperator(task_id="lambda", python_callable=main)
