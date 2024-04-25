from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import requests
import logging
from datetime import datetime, timedelta
from public_functions.slack_integration import send_slack_message

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="ETL API_CALL OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")

default_args = {
    "owner": "oci",
}

with DAG(
    "call_api_confirmation",
    default_args=default_args,
    description="Call api confirmation charge oci sucess",
    schedule_interval="0 11 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["oci", "salesforce", "api", "sinacor"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:
    
    @task
    def call(): 
        api_key = Variable.get("PASSWORD_API_CONFIRMATION")   
        headers = {
            'x-api-key': api_key,
        }

        response = requests.post('https://intranet-salesforce.ligainvest.com.br/salesforce', headers=headers)
        logging.info(response.status_code)
        logging.info(response.content)

    call()