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

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="ETL USUARIOS VERITAS OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")


default_args = {
    "owner": "oci",
}

with DAG(
    "etl_sirsan_usuarios_veritas",
    default_args=default_args,
    description="Load sirsan_tb_usuarios in adw",
    schedule_interval="0 6,18 * * 1-5",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["adw", "postgres", "usuarios"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract_transform():
        info("Starting extraction")     
        servidor = Variable.get('SQL_SERVER_PMS_SERVER')
        database = Variable.get('SQL_SERVER_PMS_DATABASE')
        usuario = Variable.get('SQL_SERVER_PMS_USER')
        senha = Variable.get('SQL_SERVER_PMS_PASSWORD')
        cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )
        select_query = "SELECT * FROM [SG2S].[dbo].[tb_Usuarios]"

        df = pd.read_sql(select_query, cnxn)

        df.fillna("null", inplace=True)
        df = df.replace([True], 'True')
        df = df.replace([False], 'False')
        df = df.replace({'\'': ''}, regex=True)
        df["DH_CARGA"] = ""
        info("Finished data transformation")
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
        df_parquet.to_parquet("oci://bkt-prod-veritas-bronze@grilxlpa4rlr/SIRSAN/TB_USUARIOS/usuarios" + datetimenow + ".parquet.gzip", compression='gzip',storage_options={"config": config},)
        logging.info("Enviado")

        logging.info("Deleção do arquivo .pem")
        if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"):
            os.remove(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem")
            logging.info(".pem deletado")
        else:
            print("Arquivo inexistente")

        return df
    
    @task()
    def load_adw(df):
        info("Starting loading into adw")

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

        cursor.execute("truncate table veritas_silver.SIRSAN_TB_USUARIOS")

        df.drop(columns="DH_CARGA", inplace=True)

        # Inserting lines
        for index,row in df.iterrows():
            try:
                str_tuple = tuple(row)
                tuple_string = ",".join(f"'{elem}'" for elem in str_tuple)
                # Add parentheses to create the desired string format
                tuple_string = f"({tuple_string})"
                
                insert_sql = (
                    "INSERT INTO "
                    + "veritas_silver.SIRSAN_TB_USUARIOS "
                    + "("
                    + lista_colunas
                    + ") "
                    + "VALUES "
                    + tuple_string
                )
                insert_sql = insert_sql[:-1] + ", SYSDATE)"

                cursor.execute(insert_sql)
                print(insert_sql)
            except Exception as e:
                print("Erro")
                print(insert_sql)
                print(e)


        connection.commit()
        connection.close()

    load_adw(load_csv(extract_transform()))