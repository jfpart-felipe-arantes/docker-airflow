import pandas as pd
import pymssql
import oracledb
import os
import datetime
from datetime import datetime, timedelta
import pandas as pd
import pendulum
import pyodbc
import pytz
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

default_args = {
    "owner": "funds",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "etl_sirsan_consulta_rentab",
    default_args=default_args,
    description="Load ",
    schedule_interval="0 23 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["SIRSAN", "Funds", "Rentab"],
) as dag:
    dag.doc_md = __doc__

    @task
    def create_engine_pymssql():
        server = Variable.get('SQL_SERVER_PMS_SERVER')
        database = Variable.get('SQL_SERVER_PMS_DATABASE')
        user = Variable.get('SQL_SERVER_PMS_USER')
        password = Variable.get('SQL_SERVER_PMS_PASSWORD')
        con = pymssql.connect(host=server,user=user,password=password,database=database)

        df = pd.read_sql("USE SGI_TRN EXEC [dbo].[sp_Consulta_Rentab_Fundo] null,2;", con)

        df["dh_carga"] = ""

        return df

    @task
    def insert_data_adw(df):
        connection = oracledb.connect(
            user = Variable.get("VERITAS_USER_PRD"),
            password = Variable.get("VERITAS_PASSWORD_PRD"),
            dsn=Variable.get("VERITAS_DNS_PRD"),
            config_dir=os.path.abspath(os.curdir) + '/wallet',
            wallet_location=os.path.abspath(os.curdir) + '/wallet',
            wallet_password=Variable.get("VERITAS_WALLET_PASSWORD")
        )

        cursor = connection.cursor()

        lista_colunas = df.columns.tolist()
        lista_colunas = ",".join(str(element) for element in lista_colunas)

        df.drop(columns="dh_carga", inplace=True)
        df = df.replace({'\'': ''}, regex=True)
        df["dt_Posicao"] = df["dt_Posicao"].astype(str)

        df.fillna("", inplace=True)

        cursor.execute("truncate table veritas_silver.SIRSAN_SP_TB_CONSULTA_RENTAB_FUNDO")

        # Inserting lines
        for index,row in df.iterrows():
            try:
                insert_sql = (
                    "INSERT INTO "
                    + "veritas_silver.SIRSAN_SP_TB_CONSULTA_RENTAB_FUNDO "
                    + "("
                    + lista_colunas
                    + ") "
                    + "VALUES "
                    + str(tuple(row))
                )
                insert_sql = insert_sql[:-1] + ", SYSDATE)"

                cursor.execute(insert_sql)
            except Exception as e:
                print("Erro")
                print(e)
        
        connection.commit()
        connection.close()

    insert_data_adw(create_engine_pymssql())