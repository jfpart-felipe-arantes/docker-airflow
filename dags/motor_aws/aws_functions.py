import pandas as pd
import boto3
from io import BytesIO, StringIO
from logging import info
from airflow.models import Variable
from datetime import datetime
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = Variable.get("S3_BUCKET")

def convert_parquet(s3_client, input_datafame, bucket_name, filepath, format):
    for coluna in input_datafame.select_dtypes(include=['datetime64']).columns:
        input_datafame[coluna] = input_datafame[coluna].dt.strftime('%Y-%m-%d %H:%M:%S')
    input_datafame["DH_CARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if format == 'parquet':
        out_buffer = BytesIO()
        input_datafame.to_parquet(out_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())

def insert_file_to_s3(aws_access_key_id,aws_secret_access_key,df,s3_bucket,s3_key):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    convert_parquet(s3_client, df, s3_bucket, s3_key, 'parquet')
    info("Sent")

def insert_historic_file_to_s3(aws_access_key_id,aws_secret_access_key,df,s3_bucket,s3_key_historic):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    convert_parquet(s3_client, df, s3_bucket, s3_key_historic, 'parquet')
    info("Sent historic")