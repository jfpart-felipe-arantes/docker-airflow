from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
from bson.json_util import dumps as mongo_dumps
import pandas as pd
import json
from public_functions.slack_integration import send_slack_message
import oracledb
from logging import info
import pendulum
import pytz
import time

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="ETL ONB TERMS OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")


default_args = {
    "owner": "oci",
}

with DAG(
    "etl_oci_onb_terms",
    default_args=default_args,
    description="Load onb_terms in oci",
    schedule_interval="0 0 * * 1-5",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["oci", "mongodb", "onb_terms"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract_transform():
        info("Starting extraction")     
        uri = Variable.get("MONGO_URL_PRD")
        
        client = MongoClient(uri)
        db = client["onboarding"]
        collection = db["onb_terms"]
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)

        df = pd.json_normalize(r)
        df["documentNumber"] = df["documentNumber"].astype('int64')
        df = pd.json_normalize(data=df.to_dict(orient='records'), record_path='terms', meta=['documentNumber', '__v', '_id.$oid', 'createdAt.$date', 'updatedAt.$date'], record_prefix="terms_")
        df.columns = df.columns.str.replace(".", "_")
        columns_to_rename = [col for col in df.columns if "_$date" in col or "_$oid" in col]

        # Rename the columns to remove "_$date" and "_$oid"
        for col in columns_to_rename:
            new_col_name = col.replace("_$date", "_date").replace("_$oid", "")
            df.rename(columns={col: new_col_name}, inplace=True)

        # Rename the column "_id" to "id" because it's not allowed to start a column name with underline
        df = df.rename(columns={"_id": "id"})
        df = df.rename(columns={"__v": "v"})

        def split_dataframe(df, chunk_size = 1000): 
            chunks = list()
            num_chunks = len(df) // chunk_size + 1
            for i in range(num_chunks):
                chunks.append(df[i*chunk_size:(i+1)*chunk_size])
            return chunks

        connection = oracledb.connect(
            user = Variable.get("VERITAS_USER_PRD"),
            password = Variable.get("VERITAS_PASSWORD_PRD"),
            dsn=Variable.get("VERITAS_DNS_PRD"),
            config_dir=os.path.abspath(os.curdir) + '/wallet',
            wallet_location=os.path.abspath(os.curdir) + '/wallet',
            wallet_password=Variable.get("VERITAS_WALLET_PASSWORD")
        )

        cursor = connection.cursor()

        df["DH_CARGA"] = str(datetime.now())

        # Creating a dict of dtypes
        dicionario_dtypes = df.dtypes.to_dict()

        # Replacing their values to datatypes of the oracle database
        repl = {"NA": None,"|O": "VARCHAR2(7000)", "<f8": "VARCHAR2(7000)", "<i8": "VARCHAR2(7000)"}
        dicionario_dtypes = {key: repl.get(value.str, value.str) for key, value in dicionario_dtypes.items()}

        # Creating create table query
        create_query = "CREATE TABLE veritas_silver.mongodb_onb_terms_teste ("
        for key, value in dicionario_dtypes.items():
            create_query += key + " " + value + ", "

        create_query = create_query[:-2]
        create_query += ")"

        cursor.execute(create_query)

        lista_colunas = df.columns.tolist()
        lista_colunas = ",".join(str(element) for element in lista_colunas)

        df = df.astype(str)
        df = df.replace({'\'': '"'}, regex=True)
        contador = 0
        splitted_df = split_dataframe(df)
        for df in splitted_df:
            insert_query = "INSERT ALL "
            for index, row in df.iterrows():
                insert_line = "INTO veritas_silver.mongodb_onb_terms_teste (" + lista_colunas + ") VALUES " + str(tuple(row))
                insert_query += insert_line

            insert_query += " SELECT * FROM dual"
            try:
                cursor.execute(insert_query)
            except Exception as E:
                print(E)
                print(insert_query)
            contador+=1000
            print(contador)
            connection.commit()

        grant_command_1 = "grant select on veritas_silver.mongodb_onb_terms_teste to mongodb"
        cursor.execute(grant_command_1)
        connection.commit()
        grant_command_2 = "grant select on veritas_silver.mongodb_onb_terms_teste to veritas_gold with grant option"
        cursor.execute(grant_command_2)
        connection.commit()

        # Perform a quick swap or rename operation
        delete_query = "DROP TABLE VERITAS_SILVER.BACKUP_MONGODB_ONB_TERMS2"
        cursor.execute(delete_query)

        cursor.execute("ALTER TABLE VERITAS_SILVER.BACKUP_MONGODB_ONB_TERMS RENAME TO BACKUP_MONGODB_ONB_TERMS2")
        connection.commit()

        cursor.execute("ALTER TABLE VERITAS_SILVER.MONGODB_ONB_TERMS RENAME TO BACKUP_MONGODB_ONB_TERMS")
        connection.commit()

        cursor.execute("ALTER TABLE VERITAS_SILVER.MONGODB_ONB_TERMS_TESTE RENAME TO MONGODB_ONB_TERMS")
        connection.commit()

        info("Finished inserting lines")

    extract_transform()