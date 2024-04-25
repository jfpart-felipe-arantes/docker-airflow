import datetime
import logging
from datetime import datetime, timedelta
from io import BytesIO, StringIO
import boto3
import pandas as pd
import pendulum
import pytz
import pyarrow as pa
from pyarrow import parquet
from io import BytesIO
from pymongo import MongoClient
from bson.json_util import dumps as mongo_dumps
import json
from logging import info
import polars as pl
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from public_functions.b3_calendar_util import is_b3_open
from public_functions.slack_integration import send_slack_message

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")


def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="bankpro_aws_raw OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")


local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
data_hora_atual = datetime.now().strftime("%d-%m-%y_%H:%M:%S")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = Variable.get("S3_BUCKET")

default_args = {
    "owner": "bankpro",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "mongo_aws_raw",
    default_args=default_args,
    description="Load mongo tables in aws",
    schedule_interval="10 7,17 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["mongo", "aws", "s3"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert,
) as dag:
    dag.doc_md = __doc__

    def onb_suitability():
        aws_access_key_id = AWS_ACCESS_KEY_ID
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
        s3_bucket = S3_BUCKET
        s3_key = f'MONGODB/PROCESSAR/ONB_SUITABILITY/ONB_SUITABILITY.parquet'
        s3_key_historico = "MONGODB/HISTORICO/ONB_SUITABILITY/ONB_SUITABILITY_" + data_hora_atual + ".parquet"
        uri = uri = Variable.get("MONGO_URL_PRD")
        client = MongoClient(uri)
        db = client["onboarding"]
        collection = db["onb_suitability"]
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)
        df = pd.json_normalize(r)
        info("Starting data transformation")
        df["DH_CARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table = pa.Table.from_pandas(df)
        parquet_buffer = BytesIO()
        parquet.write_table(table, parquet_buffer)


        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        
        parquet_buffer.seek(0)
        s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key)
        s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key_historico)
        parquet_buffer.close()
        print("arquivo no bucket")

    def onb_terms():
        aws_access_key_id = AWS_ACCESS_KEY_ID
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
        s3_bucket = S3_BUCKET
        s3_key = f'MONGODB/PROCESSAR/ONB_TERMS/ONB_TERMS.parquet'
        s3_key_historico = "MONGODB/HISTORICO/ONB_TERMS/ONB_TERMS_" + data_hora_atual + ".parquet"
        uri = Variable.get("MONGO_URL_PRD")
        client = MongoClient(uri)
        db = client["onboarding"]
        collection = db["onb_terms"]
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)
        df = pd.json_normalize(r)
        info("Starting data transformation")

        df.columns = df.columns.str.replace(".", "_")
        columns_to_rename = [col for col in df.columns if "_$date" in col or "_$oid" in col]
        # Rename the columns to remove "_$date" and "_$oid"
        for col in columns_to_rename:
            new_col_name = col.replace("_$date", "").replace("_$oid", "")
            df.rename(columns={col: new_col_name}, inplace=True)
        # Rename the column "_id" to "id" because it's not allowed to start a column name with underline
        df = df.rename(columns={"_id": "id"})
        df = df.rename(columns={"__v": "v"})
        df.fillna("null", inplace=True)
        df = df.replace([True], 'True')
        df = df.replace([False], 'False')
        df = df.replace({'\'': ''}, regex=True)
        df = df.astype(str)
        df["DH_CARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        table = pa.Table.from_pandas(df)
        parquet_buffer = BytesIO()
        parquet.write_table(table, parquet_buffer)


        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        parquet_buffer.seek(0)
        s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key)
        s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key_historico)

        parquet_buffer.close()
        print("arquivo no bucket")

    # def users():
    #     aws_access_key_id = AWS_ACCESS_KEY_ID
    #     aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    #     s3_bucket = S3_BUCKET
    #     s3_key = f'MONGODB/PROCESSAR/USERS/USERS.parquet'
    #     s3_key_historico = "MONGODB/HISTORICO/USERS/USERS_" + data_hora_atual + ".parquet"
    #     uri = Variable.get("MONGO_URL_PRD")
    #     client = MongoClient(uri)
    #     db = client["lionx"]
    #     collection = db["users"]
    #     content = mongo_dumps(collection.find({}))
    #     r = json.loads(content)
    #     df = pd.json_normalize(r)
    #     df = df.astype(str)
    #     df.columns = df.columns.str.replace(".", "_")

    #     df["DH_CARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #     table = pa.Table.from_pandas(df)
    #     parquet_buffer = BytesIO()
    #     parquet.write_table(table, parquet_buffer)


    #     s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    #     parquet_buffer.seek(0)
    #     s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key)
    #     s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key_historico)
        
    # def onb_suitability_questionaries():
    #     aws_access_key_id = AWS_ACCESS_KEY_ID
    #     aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    #     s3_bucket = S3_BUCKET
    #     s3_key = f'MONGODB/PROCESSAR/ONB_SUITABILITY_QUESTIONARIES/ONB_QUESTIONARIES.parquet'
    #     s3_key_historico = "MONGODB/HISTORICO/ONB_SUITABILITY_QUESTIONARIES/ONB_SUITABILITY_QUESTIONARIES_" + data_hora_atual + ".parquet"
    #     uri = Variable.get("MONGO_URL_PRD")
    #     client = MongoClient(uri)
    #     db = client["onboarding"]
    #     collection = db["onb_suitability_questionaries"]
    #     content = mongo_dumps(collection.find({}))
    #     r = json.loads(content)
    #     df = pd.json_normalize(r)
    #     info("Starting data transformation")
    #     def create_columns(row, index, prefix):
    #         indice = 0
    #         if isinstance(row, list) and not df.empty:
    #             for json_data in row:
    #                 for key, value in json_data.items():
    #                     new_col_name = f"{prefix}_{key}_{indice}"
    #                     if new_col_name not in df:
    #                         df[new_col_name] = None
    #                     df.at[index, new_col_name] = value                   
    #                 indice += 1
    #     df.columns = df.columns.str.replace(".", "_")
    #     prefix_list = ["questions"]
    #     # Apply the custom function to a specific column
    #     for prefix in prefix_list:
    #         df.apply(lambda row: create_columns(row[prefix], row.name, prefix=prefix), axis=1)
    #     df = df.drop(prefix_list, axis=1)
    #     filter_col = [col for col in df if col.startswith('questions_answers')]
    #     for prefix in filter_col:
    #         df.apply(lambda row: create_columns(row[prefix], row.name, prefix=prefix), axis=1)
    #     df = df.drop(filter_col, axis=1)
    #     df.columns = df.columns.str.replace(".", "_")
    #     df = df.rename(columns={"_id_$oid": "id_oid"})
    #     df.fillna("null", inplace=True)
    #     df = df.replace([True], 'True')
    #     df = df.replace([False], 'False')
    #     df = df.replace({'\'': ''}, regex=True)
        
    #     table = pa.Table.from_pandas(df)
    #     parquet_buffer = BytesIO()
    #     parquet.write_table(table, parquet_buffer)


    #     s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    #     parquet_buffer.seek(0)
    #     s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key)
    #     s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key_historico)


    onb_suitability_task = PythonOperator(
        task_id="onb_suitability", python_callable=onb_suitability
    )
    onb_terms_task = PythonOperator(
        task_id="onb_terms", python_callable=onb_terms
    )
    # users_task = PythonOperator(
    #     task_id="users", python_callable=users
    # )
    # onb_suitability_questionaries_task = PythonOperator(
    #     task_id="onb_suitability_questionaries", python_callable=onb_suitability_questionaries
    # )

    onb_suitability
    onb_terms
    # users
    # onb_suitability_questionaries