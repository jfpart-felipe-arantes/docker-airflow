import requests
import pytz
from datetime import datetime
from airflow import AirflowException
from airflow.models import Variable

timezone = pytz.timezone("America/Sao_Paulo")
webhook_url_analytics = Variable.get("WEBHOOK_URL_ANALYTICS")

def send_slack_message(*webhook_urls, slack_msg):
    """
    :param webhook_url:
    :param slack_msg:
    """
    try:
        for webhook_url in webhook_urls:
            payload = {
                "text": slack_msg,
            }
            response = requests.post(webhook_url, json=payload)
            if response.status_code != 200:
                raise Exception(
                    f"Failed to send Slack message. Status code: {response.status_code}"
                )
    except Exception as e:
        raise Exception(f"Error send_slack_message: {e}")


def send_slack_message_aws(source, table_names, message, type):
    if type == "Exception":
        slack_msg = """
<@U04V21FNP16> , <@U05LU8M4CDP>
:alert: *ERROR - {source} INGESTION - S3* :alert:
*Table Names*: {table_names}
*Dag*: {dag}
*Error*: {message}
*Execution Time*: {exec_date}
            """.format(
                    source=source,
                    table_names=table_names,
                    dag=source.lower() + "_engine",
                    message=message,
                    exec_date=datetime.now(timezone)
                )
        send_slack_message(
                    webhook_url_analytics,
                    slack_msg=slack_msg)
        raise AirflowException("The DAG has been marked as failed.")
    else:
        slack_msg = """
*Sucesso - {source} INGESTION - S3*
:white_check_mark: *Todas as tabelas foram inseridas com sucesso*
*Dag*: {dag}
*Execution Time*: {exec_date}
            """.format(
                    source=source,
                    dag=source.lower() + "_engine",
                    exec_date=datetime.now(timezone)
                )
        send_slack_message(
                    webhook_url_analytics,
                    slack_msg=slack_msg)