from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from logging import info
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
from bson.json_util import dumps as mongo_dumps
import pandas as pd
import json
import numpy as np
import oracledb
from public_functions.slack_integration import send_slack_message
import polars as pl

webhook_url_analytics = Variable.get("WEBHOOK_URL_ANALYTICS")
def task_failure_alert(context):
    send_slack_message(webhook_url_analytics, slack_msg="<@U04V21FNP16> , <@U05LU8M4CDP> :alert: *ERROR - Erro na inserção dos dados do db GANDALF, collection USERS no Veritas* :alert:")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_analytics, slack_msg="ETL Gandalf Users OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")

default_args = {
    "owner": "oci",
}


with DAG(
    "etl_gandalf_users",
    default_args=default_args,
    description="Load gandalf users in oci",
    schedule_interval="0 2 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["oci", "mongodb", "gandalf"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract():        
        uri = Variable.get("MONGO_URL_PRD")

        client = MongoClient(uri)
        db = client["gandalf"]
        collection = db["users"]
        cursor = collection.find({})
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)

        df = pd.json_normalize(r)

        df.head()

        df.columns = df.columns.str.replace(".", "_")

        df = df.rename(columns={"_id_$oid": "id_oid"})
        df = df.rename(columns={"__v": "v"})
        df.fillna("null", inplace=True)
        df = df.replace([True], 'True')
        df = df.replace([False], 'False')
        df = df.replace({'\'': ''}, regex=True)

        df["DH_CARGA"] = ""

        info(df)
        return df

    def create_pem_file():
        variable = Variable.get("KEY_OCI_PASSWORD")
        f = open(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem", "w")
        f.write(variable)
        f.close()

    @task()
    def load_csv(df):
        create_pem_file()
        config = {
        "user": "ocid1.user.oc1..aaaaaaaaflq5zmwnfyrxdcuaiwfvwgimb7bqgcje7fls475q34zes7c775rq",
        "fingerprint": "fc:85:1d:fc:c7:84:2b:a9:c5:d9:c8:bd:ed:ca:68:ca",
        "tenancy": "ocid1.tenancy.oc1..aaaaaaaampaxw5fnhyel3vfyl4puke4mkatmpusviem6vycgj43o3so6g5ua",
        "region": "sa-vinhedo-1",
        "key_file": os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"
        }
        info('Inicio Load')
        datetimenow = (datetime.now() - timedelta(hours=3)).strftime("%d_%m_%Y_%H_%M")
        df.to_csv("oci://bkt-prod-veritas-silver@grilxlpa4rlr/MONGODB/MONGODB_GANDALF/users" + datetimenow + ".csv",sep=",",index=False,storage_options={"config": config})

        info("Deleção do arquivo .pem")
        if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"):
            os.remove(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem")
            info(".pem deletado")
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
        lista_colunas = '","'.join(str(element) for element in lista_colunas)
        lista_colunas = '"' + lista_colunas + '"'

        cursor.execute("truncate table veritas_silver.MONGODB_GANDALF_USERS")

        df.drop(columns="DH_CARGA", inplace=True)

        # Inserting lines
        for index, row in df.iterrows():
            try:
                insert_line = "INSERT INTO veritas_silver.MONGODB_GANDALF_USERS (" + lista_colunas + ") VALUES " + str(tuple(row))
                insert_line = insert_line[:-1] + ", SYSDATE)"

                cursor.execute(insert_line)
                connection.commit()
                print(insert_line)

            except Exception as e:
                print("Erro no seguinte insert")
                print(insert_line)
                print(e)


        connection.commit()
        connection.close()

    load_adw(load_csv(extract()))
