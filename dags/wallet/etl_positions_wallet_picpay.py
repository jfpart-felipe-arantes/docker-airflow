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

default_args = {
    'owner': 'posvenda',
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}


with DAG(
    "etl_positions_wallet_picpay",
    default_args=default_args,
    description="Load into MongoDB Picpay postions from Bankpro and Sirsan",
    schedule_interval= '0/5 3-18 * * 1-5',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["Picpay", "Positions", "Wallet"],
    max_active_runs=1,
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
        info("Initializing data validation")

        info("Opening connection at server: " + servidor)

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

        info(df.head())
        if len(df) > 0:
            return "send_slack_message_task"
        else:
            return "is_b3_open"

    def extract_data():
    
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
        query_sirsan = f"""
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
        cursor.execute(query_sirsan)
        data_extract_sirsan = cursor.fetchall()
        info("Values from first query are OK")
        query_bankpro = """
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
        FROM [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,convert(date,getdate()),null) a
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
        cursor.execute(query_bankpro)
        data_extract_bankpro = cursor.fetchall()
        info("Values from bank pro query are OK")
        data_extract_block = []
        data_extract_block.extend(data_extract_sirsan)
        data_extract_block.extend(data_extract_bankpro)
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
                        offerId, suitabilityProfile, closedApplication, closedNewApplication, position_id, liquidationDeadline, liquidationDeadlineDaysType, update_at in data:

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


    def load_data(insert_data):
        client = MongoClient(Variable.get("MONGO_URL_WALLET_POSITIONS"))
        db = client[Variable.get("MONGO_DB_WALLET_POSITIONS")]
        collection = db[Variable.get("MONGO_COL_WALLET_POSITIONS")]
        with client.start_session() as session:
            info("Starting transaction")
            with session.start_transaction():
                collection.delete_many({}, session=session)
                info("Collection data has been deleted")
                info("Starting to insert documents in Mongo")
                collection.insert_many(insert_data, ordered=False, session=session)
                info(f"The data has been inserted into the collection, \
                        {len(insert_data)} docs")

                session.commit_transaction()

        client.close()
        info("Transaction completed")
    
    def connect_mongo(string_conn: str, database: str, collection: str):
        client = MongoClient(f"{string_conn}")
        db = client[f"{database}"]
        return db[f"{collection}"]

    def sum_values_collection_mongo(connection):
        myquery = [{'$unwind': '$assets'}, {'$group': {'_id': None, 'totalSum': {'$sum': '$assets.earnings' }}}]

        mydoc = connection.aggregate(myquery)

        totalSum = 0
        for i in mydoc:
            totalSum = i["totalSum"]

        return int(totalSum)

    def sum_values_from_list(data_transformed):
        totalSumlist = 0
        for i in range(0, len(data_transformed)):
            for j in range (0, len(data_transformed[i]["assets"])):
                totalSumlist += data_transformed[i]["assets"][j]["earnings"]

        return int(totalSumlist)

    def compare_collection_values():
        info("Initializing sum validations")
        data_extract_block = extract_data()
        data_transformed = transform_data(data_extract_block)
        sum_values_sql_server = sum_values_from_list(data_transformed)
        info("Sum from SQL server: " + str(int(sum_values_sql_server)))
        mongo_engine_picpay = connect_mongo(
            Variable.get("MONGO_URL_WALLET_POSITIONS"),
            Variable.get("MONGO_DB_WALLET_POSITIONS"),
            Variable.get("MONGO_COL_WALLET_POSITIONS"),
        )
        sum_values_mongo_picpay = sum_values_collection_mongo(mongo_engine_picpay)
        info("Sum from mongo PicPay: " + str(sum_values_mongo_picpay))
        mongo_engin_liga = connect_mongo(
            Variable.get("LIGA_MONGO_URL_WALLET_POSITIONS"),
            Variable.get("LIGA_MONGO_DB_WALLET_POSITIONS"),
            Variable.get("LIGA_MONGO_COL_WALLET_POSITIONS"),
        )
        sum_values_mongo_liga = sum_values_collection_mongo(mongo_engin_liga)
        info("Sum from mongo Liga: " + str(sum_values_mongo_liga))
        if sum_values_sql_server == sum_values_mongo_picpay == sum_values_mongo_liga:
            info("Equal values in all collections")
        else:
            info("Mismatch values in collections")
            raise AirflowException("The DAG has been marked as failed.")
            
    def main(**context):
        """
        Objetiva por extrair os dados do PMS/BankPro, transformar para um jormato json
        e persistir no mongo do ambiente picpay e liga
        """
        try:
            info("Initializing extraction")
            data_extract_block = extract_data()
            data_transformed = transform_data(data_extract_block)
            load_data(data_transformed)
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

    def send_slack_message_func(error, type, **kwargs):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - WALLET PICPAY - ETL Positions* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_positions_wallet_picpay",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - WALLET PICPAY - ETL POSITIONS*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_positions_wallet_without_previdence",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            info(slack_msg)

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

    def choose_branch(**context):
        resultado = context["task_instance"].xcom_pull(task_ids="check_null")
        if resultado:
            return "continue_task"
        return "end_task"

    check_null_values_task = BranchPythonOperator(
        task_id='check_null_values_task',
        python_callable=check_null_values
    )

    continue_task = DummyOperator(
        task_id="continue_task",
    )

    lambda_task = PythonOperator(
        task_id='lambda_task',
        python_callable=main,
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    send_slack_message_task = PythonOperator(
        task_id='send_slack_message_task',
        python_callable=send_slack_message_func,
        op_kwargs={'error': 'QUERY: "select distinct a.cpfcnpj from [SCMLionXOpen].[dbo].[Fu_ope_saldodefinitivalegado] (1,convert(date, getdate()),null) a where ValorAtualDisponivel is null" RETORNANDO VALORES', 'type': 'Exception'},
    )

    validate_sum_collections_task = PythonOperator(
        task_id='validate_sum_collections_task',
        python_callable=compare_collection_values,
    )

    check_null_values_task >> [is_b3_open_task, send_slack_message_task]
    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_task
    b3_is_open >> continue_task >> lambda_task >> validate_sum_collections_task >> end_task