import datetime
import logging
from datetime import datetime, timedelta

import pandas_market_calendars as mcal
import pendulum
import pytz
import winrm
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from public_functions.slack_integration import send_slack_message

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
webhook_url_sustentacao = Variable.get("WEBHOOK_URL_SUSTENTACAO")
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

default_args = {
    "owner": "Sirsan",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "sirsan_tributos",
    default_args=default_args,
    description="Tributos",
    schedule_interval=None,
    catchup=False,
    tags=["Sirsan", "Win", "Tributos"],
) as dag:

    def is_b3_open():
        try:
            now = datetime.now(timezone)
            b3_calendar = mcal.get_calendar("B3")
            execution_date = now.date()
            is_b3_open_check = (
                b3_calendar.valid_days(
                    start_date=execution_date, end_date=execution_date
                ).size
                > 0
            )
            if is_b3_open_check:
                print(f"B3 is open today: {execution_date}")
                return "b3_is_open"
            else:
                print(f"B3 is closed today: {execution_date}")
                return "b3_is_closed"
        except Exception as e:
            print(f"Error while checking if B3 is open: {e}")
            return "b3_is_closed"

    def conexao_rdp(command):
        host = Variable.get("IP_MAQUINA_WIN")
        username = Variable.get("USER_MAQUINA_WIN")
        password = Variable.get("PASSWORD_MAQUINA_WIN")
        session = winrm.Session(
            host,
            auth=(username, password),
            transport="ntlm",
        )
        try:
            result = session.run_ps(command)
            output = result.std_out.decode("utf-8", errors="ignore")
        except winrm.exceptions.AuthenticationError:
            output = "ERRO"
            raise ValueError(
                "Authentication error. Please check the provided username and password."
            )
        except Exception as e:
            output = "ERRO"
            raise RuntimeError(f"Error during command execution: {e}")
        finally:
            return output

    def executar_acao(**context):
        ti = context["ti"]
        current_attempts = ti.try_number
        now = datetime.now(timezone)
        execution_date = now.date()
        command = f"C:\Sirsan\BATCH\Sirsan.Console.exe EXPORTA_EASYWAY {str(execution_date)} {str(execution_date)}"
        logging.info(command)
        output = conexao_rdp(command)
        if "ERRO" in output.upper() and current_attempts <= 2:
            slack_msg = """
:red_circle: Failed {current_attempts} of 3:
*Dag*: {dag}
*Execution Time*: {exec_date}
""".format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
                current_attempts=current_attempts,
            )
            logging.error(f"Output: {output}")
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                webhook_url_sustentacao,
                slack_msg=slack_msg,
            )
            raise AirflowException("The DAG has been marked as failed.")
        elif "ERRO" in output.upper() and current_attempts > 2:
            slack_msg = """
:alert::alert::alert: Failed 3 of 3 :alert::alert::alert:
*Dag*: {dag}
*Execution Time*: {exec_date}
""".format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
            )
            logging.error(f"Output: {output}")
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                webhook_url_sustentacao,
                slack_msg=slack_msg,
            )
            trigger_sirsan_integrapms_expfundossgi.execute(context={})

            raise AirflowException("The DAG has been marked as failed.")
        elif "SUCESSO" in output.upper():
            slack_msg = """
:white_check_mark: Sucess:
*task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
""".format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
            )
            logging.error(f"Output: {output}")
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                webhook_url_sustentacao,
                slack_msg=slack_msg,
            )

    def decide_branch(**context):
        approved = context["task_instance"].xcom_pull(task_ids="is_b3_open")

        if approved:
            return "b3_is_open"
        else:
            return "b3_is_closed"

    trigger_sirsan_integrapms_expfundossgi = TriggerDagRunOperator(
        task_id="trigger_sirsan_integrapms_expfundossgi",
        trigger_dag_id="sirsan_integrapms_expfundossgi",
    )

    is_b3_open_task = BranchPythonOperator(
        task_id="is_b3_open",
        python_callable=is_b3_open,
    )

    b3_is_open = DummyOperator(
        task_id="b3_is_open",
    )

    b3_is_closed = DummyOperator(
        task_id="b3_is_closed",
    )

    EXPORTA_EASYWAY = PythonOperator(
        task_id="EXPORTA_EASYWAY",
        python_callable=executar_acao,
    )

    end_open = DummyOperator(
        task_id="end_open",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    (
        b3_is_open
        >> EXPORTA_EASYWAY
        >> end_open
        >> trigger_sirsan_integrapms_expfundossgi
    )
