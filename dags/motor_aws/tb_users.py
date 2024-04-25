import datetime
from datetime import datetime, timedelta
import pandas as pd
import pendulum
import pytz
from pymongo import MongoClient
from bson.json_util import dumps as mongo_dumps
import json
from logging import info
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from motor_aws.aws_functions import *
from public_functions.slack_integration import send_slack_message_aws

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
data_hora_atual = datetime.now().strftime("%d-%m-%y_%H:%M:%S")

default_args = {
    "owner": "bankpro",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "tb_users_to_s3",
    default_args=default_args,
    description="Load mongo tables in aws",
    schedule_interval="10 7,17 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["mongo", "aws", "s3"]
) as dag:
    dag.doc_md = __doc__

    def users():
        try:
            uri = Variable.get("MONGO_URL_PRD")
            client = MongoClient(uri)
            db = client["lionx"]
            collection = db["users"]
            content = mongo_dumps(collection.find({}))
            r = json.loads(content)
            df = pd.json_normalize(r)
            n = 500
            list_df = [df[i:i+n] for i in range(0,len(df),n)]
            contador = 0
            for df in list_df:
                s3_key = f'MONGODB/PROCESSAR/USERS/USERS' + str(contador) + '.parquet'
                df = df.astype(str)
                df.columns = df.columns.str.replace(".", "_")

                insert_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key)
                info("Users inserted in s3")
                contador+=1
        except:
            send_slack_message_aws("MONGO", 'Users', "Erro na ingestão da tabela", "Exception")

    users_task = PythonOperator(
        task_id="users", python_callable=users
    )

    users