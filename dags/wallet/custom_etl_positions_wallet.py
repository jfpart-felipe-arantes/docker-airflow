import pyodbc
import pandas as pd
import pytz
import pendulum
import pandas_market_calendars as mcal
from pymongo import MongoClient
from datetime import datetime, timedelta
from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from logging import info
from airflow.utils.dates import croniter
from public_functions.slack_integration import send_slack_message

servidor = Variable.get('SQL_SERVER_PMS_SERVER')
database = Variable.get('SQL_SERVER_PMS_DATABASE')
usuario = Variable.get('SQL_SERVER_PMS_USER')
senha = Variable.get('SQL_SERVER_PMS_PASSWORD')

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

custom_datetime = Variable.get("CUSTOM_DATETIME_POSITIONS_WALLET")

default_args = {
    'owner': 'posvenda',
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}


with DAG(
    "custom_etl_positions_wallet_picpay",
    default_args=default_args,
    description="Load into MongoDB Picpay postions from Bankpro and Sirsan setup date",
    schedule_interval= '0/5 3-18 * * 1-5',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["PMS", "Postions", "Wallet"]
) as dag:

    dag.doc_md = __doc__

    def extract_data(custom_datetime):
    
        info("Initializing extraction at server: " + servidor)
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
        cursor = conn_sql.cursor()

        info('Extracting full')
        query = f"""
                    Select 
                        RIGHT(REPLICATE('0',11) + CONVERT(VARCHAR,PE.cd_CPFCNPJ),11)	As document
                        ,DEF.id_Fundo													As id
                        ,PEP.ds_Nome													As assetDescription
                        ,null															As assetIssuer
                        ,null															As assetRentabilityDescription
                        ,sum(round(PS.vl_CotaCusto * PS.qt_Cotas, 2))					As amount
                        ,sum(round(PS.vl_Bruto - (PS.vl_CotaCusto * PS.qt_Cotas), 2))	As earnings
                        ,sum(PS.vl_Bruto)												As netValue
                        ,'InvestmentFunds' 												As 'type'
                        ,'Original' 													As 'owner'
                        ,sum(Ps.vl_IOF)  												As iof
                        ,sum(Ps.vl_IRRF) 												As irrf
                        ,def.nr_DiasConvRes 											As quoteDeadline
                        ,def.tp_DiasConvRes 											As quoteDeadlineDaysType
                        ,null 															as dueDate
                        ,null 															as dailyLiquidity      
                        ,null 															as liquidityDescription
                        ,null 															as applicationDate
                        ,null 															as gracePeriod
                        ,null 															as graceDate
                        ,null 															as productId
                        ,null                                                           as offerId
                        ,CC.Pr_NotaRisco 												as suitabilityProfile
                        ,DEF.fl_FechadoAplicacao 										as closedApplication
                        ,DEF.fl_FechadoNovaAplicacao 									as closedNewApplication
                        ,null                                                           as position_id
                        ,def.nr_DiasLiqRes											    As liquidationDeadline
    					,def.tp_DiasLiqRes												As liquidationDeadlineDaysType
                        ,getdate() 														As update_at
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
                    JOIN SGI_TRN..tb_API_Produtos cc 
                        On DEF.id_Fundo = CC.Cd_API_Mercadoria
                    Where PS.qt_cotas> '0.001'
                    Group By PE.cd_CPFCNPJ, DEF.id_Fundo, PEP.ds_Nome, DEF.nr_DiasConvRes, DEF.tp_DiasConvRes, CC.Pr_NotaRisco, DEF.fl_FechadoAplicacao, DEF.fl_FechadoNovaAplicacao, def.nr_DiasLiqRes,def.tp_DiasLiqRes 
        """ 
        cursor.execute(query)
        data_extract_pms = cursor.fetchall()
        info("Values from first query are OK")
        query_bank_pro = """
             SELECT DISTINCT 
                a.cpfcnpj AS document,
                a.custodia AS id,
                CASE
                    WHEN a.indexador = 'PRÉ' OR a.indexador is null THEN a.Papel + ' PREFIXADO'
                    ELSE a.Papel + ' PÓS-FIXADO'
                END AS assetDescription,
                a.emissor AS assetIssuer,
                CASE a.indexador
                    WHEN 'DI' THEN Cast(CONVERT(DECIMAL(5, 2), a.percentualcorrecao) AS VARCHAR) + '% do CDI'
                    WHEN 'PRÉ' THEN Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.'
                    WHEN 'IPCA' THEN 'IPCA + ' + Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.'
                    ELSE Cast(CONVERT(DECIMAL(5, 2), a.taxajuros) AS VARCHAR) + '% a.a.' 
                END AS assetRentabilityDescription,
                convert(float,a.valorprincipal) AS amount,
                isnull(convert(float,a.ValorRendtoBruto),0) AS earnings,
                isnull(convert(float,a.ValorAtual),0) AS netValue,
                'FixedIncome' as type,
                'PicPay' as Owner,
                isnull(convert(float,valoriof),0) as iof,
                isnull(convert(float,a.valorir),0) as irrf,
                NULL as quoteDeadline,
                NULL as quoteDeadlineDaysType,
                convert(DATETIME,a.vencimento) as dueDate,
                CONVERT(bit,CASE WHEN convert(date,a.Carencia) is not null THEN 
                        case when convert(date,a.Carencia) < convert(date,a.vencimento) THEN
                            case when opi.StaLiquidez is null then 'true'
                                when opi.StaLiquidez = 'S' then 'true' else 'false' end
                        else 'false' end
                    else 'false' END) AS dailyLiquidity,
                CASE WHEN convert(date,a.Carencia) is not null THEN 
                        case when convert(date,a.Carencia) < convert(date,a.vencimento) THEN
                            case when opi.StaLiquidez is null then 'Diária'
                                when opi.StaLiquidez = 'S' then 'Diária' else 'No vencimento' end
                        else 'No vencimento' end
                    else 'No vencimento' END AS liquidityDescription,
                    CONVERT(DATETIME, a.aplicacao) AS applicationDate,
                CASE
                    WHEN convert(date,a.Carencia) is null then isnull(o.PzoCarencia, 0)
                    else DATEDIFF(DAY, convert(date,a.aplicacao), CONVERT(DATETIME, a.Carencia))
                end as gracePeriod,
                CASE 
                    when convert(date,a.Carencia) is null then isnull(dateadd(DAY, o.PzoCarencia, CONVERT(DATETIME, a.aplicacao)), getdate())
                    else convert(date,a.Carencia)
                END AS graceDate,
                o.seqproduto AS productId,
                o.seqoferta as offerId,
                null as suitabilityProfile,
                null as closedApplication,
                null as closedNewApplication,
                a.custodia as position_id,
                null As liquidationDeadline,
    			null As liquidationDeadlineDaysType,
                getdate() as update_at
        FROM [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,'""" + custom_datetime + """',null) a
        left JOIN [SCMLionXOpen].[dbo].ope_produtoinvestimento opi ON opi.seqproduto = a.idproduto
        LEFT JOIN
        (SELECT seqoperacao,
        codmotivobloqueio,
        seqtitulo
        FROM [SCMLionXOpen].[dbo].vw_ope_saldooperacoesbloqueadas
        WHERE qtdbloqueada > 0 GROUP  BY seqoperacao, codmotivobloqueio, seqtitulo) b ON b.seqoperacao = a.custodia AND b.seqtitulo = a.idtitulo
        LEFT JOIN [SCMLionXOpen].[dbo].[ope_motivobloqueio] c ON c.codmotivobloqueio = b.codmotivobloqueio
        left JOIN [SCMLionXOpen].[dbo].OPE_OfertaProduto o ON o.SeqProduto = a.idProduto
        inner join (select nTitloRendaFixa, case when count (1) > 1 then 'true' else 'false' end fl_turbo from SCMLionXPricing.[dbo].tRemunTitloRendaFixa  group by nTitloRendaFixa) t on t.nTitloRendaFixa = a.idtitulo
        WHERE a.custodia IS NOT NULL
        """
        cursor.execute(query_bank_pro)
        data_extract_bank_pro = cursor.fetchall()
        info("Values from bank pro query are OK")
        data_extract_block = []
        data_extract_block.extend(data_extract_pms)
        data_extract_block.extend(data_extract_bank_pro)
        info("End of data extraction")
        conn_sql.close()
        return data_extract_block


    def transform_data(data):
        info("Initializing data transformation")
        insertData = {}
        for document, id, assetDescription, assetIssuer, \
            assetRentabilityDescription, amount, earnings, netValue, \
                type_, Owner, iof, irrf, quoteDeadline, quoteDeadlineDaysType, dueDate, dailyLiquidity, \
                    liquidityDescription, applicationDate, gracePeriod, graceDate, productId, \
                        offerId, suitabilityProfile, closedApplication, closedNewApplication, position_id, liquidationDeadline, liquidationDeadlineDaysType,update_at in data:

            if document not in insertData:
                insertData[document] = {}
                insertData[document]["document"] = ('00000000000' + document)[-11:]
                insertData[document]["assets"] = []
                insertData[document]["update_at"] = update_at
            insertData[document]["assets"].append({
                "idProduct": id,
                "assetDescription": assetDescription,
                "assetIssuer": assetIssuer,
                "assetRentabilityDescription": assetRentabilityDescription,
                "amount": amount,
                "earnings": earnings,
                "netValue": netValue,
                "type": type_,
                "Owner": Owner,
                "iof": iof,
                "irrf": irrf,
                "quoteDeadline": quoteDeadline,
                "quoteDeadlineDaysType": quoteDeadlineDaysType,
                "dueDate": dueDate,
                "dailyLiquidity": dailyLiquidity,
                "liquidityDescription": liquidityDescription,
                "applicationDate": applicationDate,
                "gracePeriod": gracePeriod,
                "graceDate": graceDate,
                "productId": productId,
                "offerId": offerId,
                "suitabilityProfile": suitabilityProfile, 
                "closedApplication": closedApplication, 
                "closedNewApplication": closedNewApplication,
                "position_id": position_id,
                "liquidationDeadline":liquidationDeadline,
                "liquidationDeadlineDaysType": liquidationDeadlineDaysType
                })
        data_to_insert = []
        for values_insert in insertData.values():
            data_to_insert.append(values_insert)
        info("End of data transformation")
        return data_to_insert

    def load_data(insert_data, collection):
        info("Deleting documents in mongo")
        collection.delete_many({})
        info("Collection data has been deleted")
        info("Starting to insert documents in Mongo")
        collection.insert_many(insert_data, ordered=False)
        info(f"The data has been inserted into the collection, \
                {len(insert_data)} docs")

    def connect_mongo(string_conn: str, database: str, collection: str):

        client = MongoClient(f"{string_conn}")
        db = client[f"{database}"]
        return db[f"{collection}"]

    def report_dag_slack(**context):
        approved = context["task_instance"].xcom_pull(task_ids="etl")
        now = datetime.now(timezone)
        next_execution_date = croniter(
            str(dag.schedule_interval), now
        ).get_next(datetime)
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
        """
        Objetiva por extrair os dados do PMS/BankPro, transformar para um jormato json
        e persistir no mongo do ambiente picpay e liga
        """
        now = datetime.now(timezone)
        next_execution_date = croniter(
            str(dag.schedule_interval), now
        ).get_next(datetime)
        ti = context["ti"]
        current_attempts = ti.try_number
        try:
            mongo_engine = connect_mongo(
                Variable.get("MONGO_URL_WALLET_POSITIONS"),
                Variable.get("MONGO_DB_WALLET_POSITIONS"),
                Variable.get("MONGO_COL_WALLET_POSITIONS"),
            )
            info("Initializing extraction")
            data_extract_block = extract_data(custom_datetime)
            data_transformed = transform_data(data_extract_block)
            load_data(data_transformed, mongo_engine)
            return "sucess"
        except Exception as e:
            info("ETL error", e)
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

    lambda_task = PythonOperator(
        task_id='lambda_task',
        python_callable=main,
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    lambda_task >> end_task