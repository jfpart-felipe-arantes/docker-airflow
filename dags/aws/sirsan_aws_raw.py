import pandas as pd
import boto3
from io import BytesIO, StringIO
import pyodbc
from datetime import datetime
import datetime
import logging
from datetime import datetime, timedelta
import pendulum
import pyodbc
import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from public_functions.slack_integration import send_slack_message
from public_functions.b3_calendar_util import is_b3_open

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="bankpro_aws_raw OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")


local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
data_hora_atual = datetime.now().strftime('%d-%m-%y_%H:%M:%S')
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = Variable.get("S3_BUCKET")

default_args = {
    "owner": "sirsan",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "sirsan_aws_raw",
    default_args=default_args,
    description="Load sirsan tables in aws",
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["bankpro", "aws", "s3"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:
    dag.doc_md = __doc__


    def create_connection():
        servidor_sqlserver = Variable.get("FIXED_INCOME_SQLSERVER_HOST")
        database = "scmlionxopen"
        usuario = Variable.get("FIXED_INCOME_SQLSERVER_USER")
        senha = Variable.get("FIXED_INCOME_SQLSERVER_PASSWORD")
        cnxn_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor_sqlserver
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )
        cnxn = pyodbc.connect(cnxn_string)
        logging.info("Created connection")

        return cnxn

    def template_connection(cnxn, source_table):
        df = pd.read_sql(f""" SELECT * FROM {source_table}""",
            cnxn
            )
        return df

    def end_connection(cnxn, **kwargs):
        cnxn.close()

    def convert_parquet(s3_client, input_datafame, bucket_name, filepath, format):
        if format == 'parquet':
            out_buffer = BytesIO()
            input_datafame.to_parquet(out_buffer, index=False)
        elif format == 'csv':
            out_buffer = StringIO()
            input_datafame.to_parquet(out_buffer, index=False)          
        s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())

    def insert_file_to_s3(aws_access_key_id,aws_secret_access_key,df,s3_bucket,s3_key):
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        convert_parquet(s3_client, df, s3_bucket, s3_key, 'parquet')
        logging.info("Sent")

    def insert_historic_file_to_s3(aws_access_key_id,aws_secret_access_key,df,s3_bucket,s3_key_historic):
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        convert_parquet(s3_client, df, s3_bucket, s3_key_historic, 'parquet')
        logging.info("Sent historic")

    def tb_pasaldo(cnxn, **kwargs):
        logging.info("Start extraction tb_pasaldo")

        source_table = (
            "SGI_TRN.dbo.TB_PASSALDO"
        )
        s3_key = f'SIRSAN/PROCESSAR/TB_PASSALDO/TB_PASSALDO.parquet'
        s3_key_historico = 'SIRSAN/HISTORICO/TB_PASSALDO/TB_PASSALDO_' + data_hora_atual + '.parquet'

        df = template_connection(cnxn, source_table)

        logging.info("Created DataFrame")

        insert_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key)
        insert_historic_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key_historico)

        logging.info("tb_pasaldo Data inserted")

    def tb_pasaldo_his(cnxn, **kwargs):
        logging.info("Start extraction tb_pasaldo_his")

        source_table = (
            "SGI_TRN.dbo.TB_PASSALDO_his"
        )
        s3_key = f'SIRSAN/PROCESSAR/TB_PASSALDO_HIS/TB_PASSALDO_HIS.parquet'
        s3_key_historico = f'SIRSAN/HISTORICO/TB_PASSALDO_HIS/TB_PASSALDO_HIS_' + data_hora_atual + '.parquet'

        df = template_connection(cnxn, source_table)

        logging.info("Created DataFrame")

        insert_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key)
        insert_historic_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key_historico)

        logging.info("tb_pasaldo_his Data inserted")

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

    cnxn = create_connection()

    end_open = DummyOperator(
        task_id="end_open",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    tb_pasaldo_task = PythonOperator(
        task_id="tb_pasaldo", python_callable=tb_pasaldo, op_kwargs={"cnxn": cnxn}
    )
    tb_pasaldo_his_task = PythonOperator(
        task_id="tb_pasaldo_his", python_callable=tb_pasaldo_his, op_kwargs={"cnxn": cnxn}
    )
    end_connection_task = PythonOperator(
        task_id="end_connection", python_callable=end_connection, op_kwargs={"cnxn": cnxn}
    )


    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    (
        b3_is_open
        >> tb_pasaldo_task
        >> tb_pasaldo_his_task
        >> end_connection_task
        >> end_open
    )