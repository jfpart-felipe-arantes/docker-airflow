import datetime
from datetime import datetime, timedelta
from logging import info

import pendulum
import pytz
from airflow import DAG
from airflow.decorators import task
from motor_aws.aws_functions import *
from motor_aws.bankpro_sqlserver import *
from motor_aws.table_source import *    

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
data_hora_atual = datetime.now().strftime("%d-%m-%y_%H:%M:%S")

default_args = {
    "owner": "bankpro",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "tb_precocurva_to_s3",
    default_args=default_args,
    description="Load precocurvatitulolegado table in aws",
    schedule_interval="30 5,18 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["bankpro", "aws", "s3"],
    max_active_runs=1
) as dag:
    dag.doc_md = __doc__

    @task
    def execute():
        bankpro = Bankpro_class()
        connection = bankpro.create_connection()

        chunks = bankpro.get_rows_count("SELECT count(*) FROM SCMLionXOpen.dbo.vw_oci_fu_ope_precocurvatitulolegado", connection)
        chunks = chunks.values[0][0] // 35 + 35
        contador = 0

        query = 'select IdTitulo, DataEmissao, DataVencimento, DataCurva, PuCurvaAbertura, Titulo from SCMLionXOpen.dbo.vw_oci_fu_ope_precocurvatitulolegado'
        for chunk_dataframe in pd.read_sql(query, connection, chunksize=chunks):
            chunk_dataframe = chunk_dataframe.astype(str)
            s3_key = ("BANKPRO/PROCESSAR/FU_OPE_PRECOCURVATITULOLEGADO/FU_OPE_PRECOCURVATITULOLEGADO" + str(contador)  + ".parquet")
            insert_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, chunk_dataframe, S3_BUCKET, s3_key)
            print(contador)
            contador+=1
            connection = bankpro.create_connection()

    execute()