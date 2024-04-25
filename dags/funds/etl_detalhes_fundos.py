import datetime
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
    "etl_detalhes_fundos",
    default_args=default_args,
    description="Load details funds into PostgreSQL from PMS",
    schedule_interval="0,30 6-18 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["PMS", "Funds", "Details"],
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
        SELECT distinct
            id_Fundo as cd_Externo,
            id_Fundo as id_AtivoFundos,
            ds_FundoClube 'nomeFundo',
            CNPJ 'cnpjFundo',
            DF.dt_atual 'dataAtual',
            DF.ds_ClasseCVM 'classeCVM' ,
            (Select	H.vl_Cota
                            From	SGI_TRN.dbo.tb_HisCotas H (Nolock)
                            Where	H.id_Portfolio = DF.id_FundoClube
                            And		H.dt_Cota = (Select Max(dt_Cota)
                                                From SGI_TRN.dbo.tb_HisCotas H2 (NoLock)
                                                Where H2.id_Portfolio = DF.id_FundoClube) ) 'valorCota',
            DF.vl_MinResg 'valorMinResg',
            vl_MinPerma 'valorMinPermanencia',
            DF.nr_DiasConvRes 'diasConversaoResg',
            DF.nr_DiasConvApl 'dias_conversao_aplic',
            case
                tp_DiasConvApl when 'N' then 'U'
                else 'C'
                end 'tipo_conversao_aplic',
            CASE
                DF.tp_DiasConvRes
                WHEN 'N' THEN 'U'
                ELSE 'C'
            END 'tipoConversaoResg',
            DF.nr_DiasLiqRes 'diasLiquidacaoResg',
            CASE
                DF.tp_DiasLiqRes
                WHEN 'N'
                    THEN 'U'
                ELSE 'C'
            END 'tipoLiquidacaoResg',
            DF.dt_HoraIniResg 'horaInicioResg',
            DF.dt_HoraFimResg 'horaFimResg',
            DF.vl_Taxa 'taxaADMMin',
            DF.vl_PGBL 'taxaADMMax',
            DF.pr_PercentualTaxasPfee 'taxaPerformanceIndice',
            DF.pr_TaxaPerfTaxasPfee 'taxaPerformanceGestor',
            DF.tp_tributacao 'tipoTributacao',
            DF.ds_ApelidoCustodiante 'ds_ApelidoCustodiante',
            vl_RfCpAte6 'rfCPate6',
            vl_RfCpApos6 'rfCPapos6',
            vl_RfCpComecotas 'rfCPComeCotas',
            vl_RfAte6 'rfate6',
            vl_RfDe6a12 'rpDe6a12',
            vl_RfDe12a24 'rfDe12a24',
            vl_RfApos24 'rfApos24',
            vl_RfComeCotas 'rfComeCotas',
            vl_RvAte2001 'rvAte2001',
            vl_RvApos2001 'rvApos2001',
            vl_RvGanho 'rvGanho',
            vl_RvOpNormais 'rvOpNormais',
            vl_RvOpDayTrade 'rvOpDayTrade',
            vl_DebIncentivadasPF 'debenturesIncentivadasPF',
            vl_DebIncentivadasPJ 'debenturesIncentivadasPJ',
            vl_DebIncentivadasPFExterior 'debenturesIncentivadasPFExterior',
            vl_DebIncentivadasPJExterior 'debenturesIncentivadasPJExterior',
            vl_DebIncentivadasPFExteriorParaiso 'debenturesIncentivadasPFExteriorParaiso',
            vl_DebIncentivadasPJExteriorParaiso 'debenturesIncetivadasPJExteriorParaiso',
            df.id_FundoClube 'idFundoClube',
            df.dt_Inicio 'inicioFundo',
            df.ds_Indice 'indiceReferencia',
            df.ds_ApelidoGestor 'gestor',
            df.ds_ApelidoAdministrador 'administrador',
            df.vl_MinAplIni 'minAplicacaoInicial',
            df.vl_MinAplAdc 'minMovimentacao',
            df.fl_Qualificado 'flQualificado',
            df.fl_FechadoAplicacao 'flFechadoAplicacao',
            df.fl_FechadoNovaAplicacao 'flFechadoNovaAplicacao',
            df.dt_HoraIniAplic 'horaInicioAplicacao',
            df.dt_HoraFimAplic 'horaFimAplicacao',
            DF.vl_PGBL 'taxaADMMax',
	        P.cd_Sinacor 'cd_sinacor',
	        CC.Pr_NotaRisco 'cd_suitability_profile'
        from SGI_TRN.dbo.vw_DetalheFundo DF
	  	JOIN SGI_TRN..tb_API_Produtos cc on DF.id_Fundo = CC.Cd_API_Mercadoria
		JOIN SGI_TRN..tb_AtvProduto JJ on JJ.id_Produto = CC.Cd_API_Produto
		JOIN SGI_TRN..tb_CadPortfolio P ON P.id_FundoClube = DF.id_fundo
            """
        try:
            cursor.execute(query)
            values = cursor.fetchall()
            for value in values:
                print(value["idFundoClube"])
                query2 = f"""exec SGI_TRN.dbo.sp_Consulta_Rentab_Fundo @p_id_Fundo = {value['idFundoClube']}"""
                cursor.execute(query2)
                result = cursor.fetchone()
                if result is not None:
                    patrimonio_liquido_value = result["vl_PlInicioDia"]
                    rentabilidade_mes_value = result["vl_RentabMes"]
                    rentabilidade_ano_value = result["vl_RentabAno"]
                    rentabilidade_12_meses_value = result["vl_RentabUlt12"]
                    value.update(
                        {
                            "vl_PlInicioDia": patrimonio_liquido_value,
                            "vl_RentabMes": rentabilidade_mes_value,
                            "vl_RentabAno": rentabilidade_ano_value,
                            "vl_RentabUlt12": rentabilidade_12_meses_value,
                        }
                    )
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

    def process_dataframe(df):
        field_mapping = {
            "administrador": "administrador",
            "cd_Externo": "cd_externo",
            "cd_sinacor": "cd_sinacor",
            "cd_suitability_profile": "cd_suitability_profile",
            "classeCVM": "classe_cvm",
            "cnpjFundo": "cnpj_fundo",
            "dataAtual": "data_atual",
            "debenturesIncentivadasPF": "debentures_incentivadas_pf",
            "debenturesIncentivadasPFExterior": "debentures_incentivadas_pf_exterior",
            "debenturesIncentivadasPFExteriorParaiso": "debentures_incentivadas_pf_exterior_paraiso",
            "debenturesIncentivadasPJ": "debentures_incentivadas_pj",
            "debenturesIncentivadasPJExterior": "debentures_incentivadas_pj_exterior",
            "debenturesIncetivadasPJExteriorParaiso": "debentures_incetivadas_pj_exterior_paraiso",
            "diasConversaoResg": "dias_conversao_resg",
            "diasLiquidacaoResg": "dias_liquidacao_resg",
            "ds_ApelidoCustodiante": "custodiante",
            "flFechadoAplicacao": "fl_fechado_aplicacao",
            "flFechadoNovaAplicacao": "fl_fechado_nova_aplicacao",
            "flQualificado": "fl_qualificado",
            "gestor": "gestor",
            "horaFimAplicacao": "hora_fim_aplicacao",
            "horaFimResg": "hora_fim_resg",
            "horaInicioAplicacao": "hora_inicio_aplicacao",
            "horaInicioResg": "hora_inicio_resg",
            "idFundoClube": "id_fundo_clube",
            "id_AtivoFundos": "id_ativo_fundos",
            "indiceReferencia": "indice_referencia",
            "inicioFundo": "inicio_fundo",
            "minAplicacaoInicial": "min_aplicacao_inicial",
            "minMovimentacao": "min_movimentacao",
            "nomeFundo": "nome_fundo",
            "dias_conversao_aplic": "dias_conversao_aplic",
            "rfApos24": "rf_apos24",
            "rfCPComeCotas": "rf_cp_come_cotas",
            "rfCPapos6": "rf_cp_apos6",
            "rfCPate6": "rf_cp_ate6",
            "rfComeCotas": "rf_come_cotas",
            "rfDe12a24": "rf_de12a24",
            "rfate6": "rf_ate6",
            "rpDe6a12": "rp_de6a12",
            "rvApos2001": "rv_apos2001",
            "rvAte2001": "rv_ate2001",
            "rvGanho": "rv_ganho",
            "rvOpDayTrade": "rv_op_day_trade",
            "rvOpNormais": "rv_op_normais",
            "taxaADMMax": "taxa_adm_max",
            "taxaADMMin": "taxa_adm_min",
            "taxaPerformanceGestor": "taxa_performance_gestor",
            "taxaPerformanceIndice": "taxa_performance_indice",
            "tipoConversaoResg": "tipo_conversao_resg",
            "tipoLiquidacaoResg": "tipo_liquidacao_resg",
            "tipoTributacao": "tipo_tributacao",
            "tipo_conversao_aplic": "tipo_conversao_aplic",
            "valorCota": "valor_cota",
            "valorMinPermanencia": "valor_min_permanencia",
            "valorMinResg": "valor_min_resg",
            "vl_PlInicioDia": "patrimonio_liquido",
            "vl_RentabAno": "rentabilidade_ano",
            "vl_RentabMes": "rentabilidade_mes",
            "vl_RentabUlt12": "rentabilidade_12_meses",
        }
        created_date = datetime.utcnow()
        df_renamed = df.rename(columns=field_mapping)
        df_renamed["created_at"] = created_date
        coluns_detalhes_fundos = {
            "administrador",
            "cd_externo",
            "cd_sinacor",
            "cd_suitability_profile",
            "classe_cvm",
            "cnpj_fundo",
            "created_at",
            "custodiante",
            "data_atual",
            "debentures_incentivadas_pf",
            "debentures_incentivadas_pf_exterior",
            "debentures_incentivadas_pf_exterior_paraiso",
            "debentures_incentivadas_pj",
            "debentures_incentivadas_pj_exterior",
            "debentures_incetivadas_pj_exterior_paraiso",
            "dias_conversao_aplic",
            "dias_conversao_resg",
            "dias_liquidacao_resg",
            "fl_fechado_aplicacao",
            "fl_fechado_nova_aplicacao",
            "fl_qualificado",
            "gestor",
            "hora_fim_aplicacao",
            "hora_fim_resg",
            "hora_inicio_aplicacao",
            "hora_inicio_resg",
            "id_ativo_fundos",
            "id_fundo_clube",
            "indice_referencia",
            "inicio_fundo",
            "min_aplicacao_inicial",
            "min_movimentacao",
            "nome_fundo",
            "patrimonio_liquido",
            "rentabilidade_12_meses",
            "rentabilidade_ano",
            "rentabilidade_mes",
            "rf_apos24",
            "rf_ate6",
            "rf_come_cotas",
            "rf_cp_apos6",
            "rf_cp_ate6",
            "rf_cp_come_cotas",
            "rf_de12a24",
            "rp_de6a12",
            "rv_apos2001",
            "rv_ate2001",
            "rv_ganho",
            "rv_op_day_trade",
            "rv_op_normais",
            "taxa_adm_max",
            "taxa_adm_min",
            "taxa_performance_gestor",
            "taxa_performance_indice",
            "tipo_conversao_aplic",
            "tipo_conversao_resg",
            "tipo_liquidacao_resg",
            "tipo_tributacao",
            "valor_cota",
            "valor_min_permanencia",
            "valor_min_resg",
        }
        colunas_comuns = [
            coluna
            for coluna in df_renamed.columns
            if coluna in coluns_detalhes_fundos
        ]
        df_final = df_renamed[colunas_comuns]

        return df_final

    def load_to_postgesql(df):
        db_host = Variable.get("POSTGRES_FUNDS_HOST")
        db_name = Variable.get("POSTGRES_FUNDS_NAME")
        db_password = Variable.get("POSTGRES_FUNDS_PASSWORD")
        db_port = Variable.get("POSTGRES_FUNDS_PORT")
        db_user = Variable.get("POSTGRES_FUNDS_USER")

        postgresql_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        try:
            with create_engine(postgresql_url).connect() as conn:
                df.to_sql(
                    "detalhes_fundos",
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
        finally:
            conn.close()

    def send_slack_message_func(error, type):
        if type == "Exception":
            slack_msg = """
    <@U04V21FNP16> , <@U05LU8M4CDP>
    :alert: *ERROR - DETALHES FUNDOS* :alert:
    *Dag*: {dag}
    *Error*: {error}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_detalhes_fundos",
                        error=error,
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            raise AirflowException("The DAG has been marked as failed.")
        else:
            slack_msg = """
    :white_check_mark: *SUCCESS - DETALHES FUNDOS*
    *Dag*: {dag}
    *Execution Time*: {exec_date}
                """.format(
                        dag="etl_detalhes_fundos",
                        exec_date=datetime.now(timezone)
                    )
            send_slack_message(webhook_url_fundos,
                        webhook_url_engineer,
                        slack_msg=slack_msg)
            
    def main(**context):
        try:
            query_results = extract_data()
            processed_df = process_dataframe(query_results)
            load_to_postgesql(processed_df)
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
