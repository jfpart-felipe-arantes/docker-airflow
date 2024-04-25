from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
import logging
from datetime import datetime, timedelta
import os
import pandas as pd
from public_functions.slack_integration import send_slack_message
import oracledb
from logging import info
import pendulum
import pytz
import pyodbc
import psycopg2

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="Offers to parquet OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")


default_args = {
    "owner": "oci",
}

with DAG(
    "etl_oci_fixed_income_offers",
    default_args=default_args,
    description="Load fixed income offers in bucket OCI",
    schedule_interval="2 10 * * 1-5",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["bucket", "bankpro", "fixed_income_offers", "oci"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract_data_sqlserver():
        servidor_sqlserver = Variable.get("SQL_SERVER_PMS_SERVER_PRD")
        database = Variable.get("SQL_SERVER_PMS_DATABASE_PRD")
        usuario = Variable.get("SQL_SERVER_PMS_USER_PRD")
        senha = Variable.get("SQL_SERVER_PMS_PASSWORD_PRD")
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
        
        query = """SELECT  o.seqoferta AS id_offer,
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
        and o.DtaValidade = convert(date,getdate())"""
        df = pd.read_sql_query(query, cnxn)
        cnxn.close()
        return df

    
    def create_pem_file():
        variable = Variable.get("KEY_OCI_PASSWORD")
        f = open(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem", "w")
        f.write(variable)
        f.close()

    @task()
    def load_csv(df):
        df_parquet = df.copy()
        for coluna in df_parquet.columns:
            df_parquet[coluna] = df_parquet[coluna].astype(str)
        info("Creating pem file")
        create_pem_file()
        config = {
        "user": "ocid1.user.oc1..aaaaaaaaflq5zmwnfyrxdcuaiwfvwgimb7bqgcje7fls475q34zes7c775rq",
        "fingerprint": "fc:85:1d:fc:c7:84:2b:a9:c5:d9:c8:bd:ed:ca:68:ca",
        "tenancy": "ocid1.tenancy.oc1..aaaaaaaampaxw5fnhyel3vfyl4puke4mkatmpusviem6vycgj43o3so6g5ua",
        "region": "sa-vinhedo-1",
        "key_file": os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"
        }
        datetimenow = (datetime.now() - timedelta(hours=3)).strftime("%d_%m_%Y_%H_%M")
        logging.info('Inicio Load')
        df_parquet.to_parquet("oci://bkt-prod-veritas-bronze@grilxlpa4rlr/BANKPRO/FIXED_INCOME_OFFERS/fixed_income_offers.parquet.gzip", compression='gzip',storage_options={"config": config},)
        logging.info("Enviado")

        logging.info("Deleção do arquivo .pem")
        if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"):
            os.remove(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem")
            logging.info(".pem deletado")
        else:
            print("Arquivo inexistente")

        return df
    load_csv(extract_data_sqlserver())