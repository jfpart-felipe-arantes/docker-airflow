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
from airflow.utils.dates import croniter
from public_functions.slack_integration import send_slack_message

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
webhook_url_sustentacao = Variable.get("WEBHOOK_URL_SUSTENTACAO")
webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
webhook_url_fundos = Variable.get("WEBHOOK_URL_FUNDOS")

default_args = {
    "owner": "Sirsan",
    "description": "Check the existence of a file and then send an email to the manager",
    "retries": 2,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "sirsan_send_email",
    default_args=default_args,
    description="Contabil",
    schedule_interval="*/10 13-16 * * 1-5",
    catchup=False,
    tags=["Sirsan", "Win", "Gestor", "E-mail"],
) as dag:
    dag.doc_md = __doc__

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
                logging.info(f"B3 is open today: {execution_date}")
                return "b3_is_open"
            else:
                logging.warning(f"B3 is closed today: {execution_date}")
                return "b3_is_closed"
        except Exception as e:
            logging.warning(f"Error while checking if B3 is open: {e}")
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

    def check_is_empty(**context):
        now = datetime.now(timezone)
        next_execution_date = croniter(
            str(dag.schedule_interval), now
        ).get_next(datetime)
        ti = context["ti"]
        current_attempts = ti.try_number
        command = f"dir C:\Sirsan\Doc_email"
        logging.info(command)
        output = conexao_rdp(command)
        logging.info(f"dir results: {output}")
        if "ERRO" in output.upper() and current_attempts <= 2:
            slack_msg = """
:red_circle: Failed {current_attempts} of 3:
*task*: {task}
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
*task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
*Next run:* {next_execution_date} :alarm_clock:
                """.format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
                next_execution_date=next_execution_date,
            )
            logging.error(f"Output: {output}")
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                webhook_url_sustentacao,
                slack_msg=slack_msg,
            )
            raise AirflowException("The DAG has been marked as failed.")

        if str(output) is not "":
            logging.info(f"Not empty")
            return "not_empty"
        else:
            logging.warning(f"Is empty")
            slack_msg = """
The folder doc_email is empty. :open_file_folder:
*task*: {task}
*Dag*: {dag}
*Execution Time*: {exec_date}
*Next run:* {next_execution_date} :alarm_clock:
                            """.format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
                next_execution_date=next_execution_date,
            )
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                webhook_url_sustentacao,
                slack_msg=slack_msg,
            )
            return "is_empty"

    def send_email(**context):
        now = datetime.now(timezone)
        next_execution_date = croniter(
            str(dag.schedule_interval), now
        ).get_next(datetime)
        ti = context["ti"]
        current_attempts = ti.try_number
        command = f"C:\Sirsan\BATCH\Sirsan.Console.exe ENVIAR_EMAIL S"
        logging.info(command)
        output = conexao_rdp(command)
        if "ERRO" in output.upper() and current_attempts <= 2:
            slack_msg = """
:red_circle: Failed {current_attempts} of 3:
*task*: {task}
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
*Next run:* {next_execution_date} :alarm_clock:
        """.format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
                next_execution_date=next_execution_date,
            )
            logging.error(f"Output: {output}")
            send_slack_message(
                webhook_url_fundos,
                webhook_url_engineer,
                webhook_url_sustentacao,
                slack_msg=slack_msg,
            )
            raise AirflowException("The DAG has been marked as failed.")
        elif "SUCESSO" in output.upper():
            slack_msg = """
:white_check_mark: Sucess:
*Dag*: {dag}
*Execution Time*: {exec_date}
*Next run:* {next_execution_date} :alarm_clock:
        """.format(
                task=context.get("task_instance").task_id,
                dag=context.get("task_instance").dag_id,
                ti=context.get("task_instance"),
                exec_date=now,
                next_execution_date=next_execution_date,
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

    def decide_branch_empty_folder(**context):
        check_empty = context["task_instance"].xcom_pull(
            task_ids="check_is_empty_task"
        )
        if check_empty:
            return "is_empty_task"
        else:
            return "not_empty_task"

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

    check_is_empty_task = BranchPythonOperator(
        task_id="check_is_empty",
        python_callable=check_is_empty,
    )

    send_email_task = PythonOperator(
        task_id="send_email_gestor",
        python_callable=send_email,
    )

    not_empty_task = DummyOperator(
        task_id="not_empty",
    )

    is_empty_task = DummyOperator(
        task_id="is_empty",
    )

    end_send_email_task = DummyOperator(
        task_id="end_send_email",
    )
    end_is_empty = DummyOperator(
        task_id="end_is_empty",
    )

    end_closed = DummyOperator(
        task_id="end_closed",
    )

    is_b3_open_task >> b3_is_open
    is_b3_open_task >> b3_is_closed >> end_closed
    b3_is_open >> check_is_empty_task
    (
        check_is_empty_task
        >> not_empty_task
        >> send_email_task
        >> end_send_email_task
    )
    check_is_empty_task >> is_empty_task >> end_is_empty
