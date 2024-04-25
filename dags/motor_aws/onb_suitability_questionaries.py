from datetime import datetime
import datetime
from logging import info
from datetime import datetime, timedelta
import pendulum
import pytz
import json
from bson.json_util import dumps as mongo_dumps
from pymongo import MongoClient
from airflow import DAG
from airflow.decorators import task
from public_functions.slack_integration import send_slack_message
from motor_aws.aws_functions import *
from motor_aws.sinacor_oracle import *

webhook_url_analytics = Variable.get("WEBHOOK_URL_ANALYTICS")

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
data_hora_atual = datetime.now().strftime('%d-%m-%y_%H:%M:%S')

default_args = {
    "owner": "mongodb",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "load_onb_suitability_questionaries",
    default_args=default_args,
    description="Load mongodb tables in aws",
    schedule_interval="0 7 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["mongodb", "aws", "s3"]
) as dag:
    dag.doc_md = __doc__

    @task
    def extract():
        uri = Variable.get("MONGO_URL_PRD")

        client = MongoClient(uri)
        db = client["onboarding"]
        collection = db["onb_suitability_questionaries"]
        cursor = collection.find({})
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)

        df = pd.json_normalize(r)

        def create_columns(row, index, prefix):
            indice = 0
            if isinstance(row, list) and not df.empty:
                for json_data in row:
                    for key, value in json_data.items():
                        new_col_name = f"{prefix}_{key}_{indice}"
                        if new_col_name not in df:
                            df[new_col_name] = None
                        df.at[index, new_col_name] = value
                    
                    indice += 1

        df.columns = df.columns.str.replace(".", "_")


        prefix_list = ["questions"]

        # Apply the custom function to a specific column
        for prefix in prefix_list:
            df.apply(lambda row: create_columns(row[prefix], row.name, prefix=prefix), axis=1)

        df = df.drop(prefix_list, axis=1)

        filter_col = [col for col in df if col.startswith('questions_answers')]

        for prefix in filter_col:
            df.apply(lambda row: create_columns(row[prefix], row.name, prefix=prefix), axis=1)

        df = df.drop(filter_col, axis=1)

        df.columns = df.columns.str.replace(".", "_")

        df = df.rename(columns={"_id_$oid": "id_oid"})
        df.fillna("null", inplace=True)
        df = df.replace([True], 'True')
        df = df.replace([False], 'False')
        df = df.replace({'\'': ''}, regex=True)
        
        return df

    @task
    def load(df):
        try:
            table_name = 'ONB_SUITABILITY_QUESTIONARIES'

            s3_key = 'MONGODB/PROCESSAR/' + table_name + '/' + table_name + '.parquet'
            s3_key_historico = 'MONGODB/HISTORICO/' + table_name + '/' + table_name + '_' + data_hora_atual + '.parquet'
            
            insert_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key)
            insert_historic_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key_historico)
            send_slack_message(webhook_url_analytics, slack_msg=":white_check_mark: *SUCCESS - Collection ONB_SUITABILITY_QUESTIONARIES inserida no S3*")
        except:
            send_slack_message(webhook_url_analytics, slack_msg="<@U04V21FNP16> , <@U05LU8M4CDP> :alert: *ERROR - Erro na inserção dos dados da collection ONB_SUITABILITY_QUESTIONARIES no S3* :alert:")

    load(extract())