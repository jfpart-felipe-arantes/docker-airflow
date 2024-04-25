from datetime import datetime
import datetime
from logging import info
from datetime import datetime, timedelta
import pendulum
import pytz
from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.operators.python import PythonOperator
from public_functions.slack_integration import send_slack_message_aws
from motor_aws.aws_functions import *
from motor_aws.sirsan_sql_server import *
from motor_aws.table_source import *    
from airflow.utils.trigger_rule import TriggerRule

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")
data_hora_atual = datetime.now().strftime('%d-%m-%y_%H:%M:%S')
execution_date = datetime.now()
default_args = {
    "owner": "sirsan",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "start_date": pendulum.datetime(2023, 8, 14, tz=local_tz),
}

with DAG(
    "sirsan_engine_to_s3",
    default_args=default_args,
    description="Load sirsan tables in aws",
    schedule_interval="40 5,18 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["sirsan", "aws", "s3"],
    max_active_runs=1,
    concurrency=3
) as dag:
    dag.doc_md = __doc__

    def template(table_name, query, connection, task_id, **kwargs):
        try:
            df = pd.read_sql(query, connection)
            s3_key = 'SIRSAN/PROCESSAR/' + table_name + "/" + table_name + ".parquet"
            info("Created DataFrame")
            insert_file_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, df, S3_BUCKET, s3_key)
            info(table_name + " inserted in s3")
        except Exception as E:
            info(E)
            kwargs['ti'].xcom_push(key=f'{task_id}_result', value=table_name)


    def slack_task_function(**kwargs):
        ti = kwargs['ti']
        
        dag_run = kwargs['dag_run']
        execution_date = dag_run.execution_date
        lista_falha = []
        for dynamic_task in dynamic_tasks:
            taskinstance = TaskInstance(task=dynamic_task, execution_date=execution_date)
            task_state = taskinstance.current_state()
            result_key = f'{dynamic_task.task_id}_result'
            result_value = ti.xcom_pull(task_ids=dynamic_task.task_id, key=result_key)
            info(dynamic_task.task_id)
            info(task_state)
            if result_value != None or task_state == State.FAILED:
                lista_falha.append(dynamic_task.task_id)
        
        if len(lista_falha) == 0:
            send_slack_message_aws("SIRSAN", "", "", "Ok")
        else:
            send_slack_message_aws("SIRSAN", lista_falha, "Erro na ingestão das tabelas", "Exception")

        info(f"Final results: {lista_falha}")

    sirsan = Sirsan_class()
    connection = sirsan.create_connection()

    df = postgres_create_dataframe('SIRSAN')

    dynamic_tasks = []
    for index, row in df.iterrows():
        task_id= row["TABELA"]
        dynamic_task  = PythonOperator(task_id=task_id, 
                    python_callable=template,
                    op_kwargs={"table_name": row["TABELA"], "query": row["QUERY"], "connection": connection, "task_id": task_id},
                    )
        dynamic_tasks.append(dynamic_task)

    slack_message_task = PythonOperator(
        task_id='slack_message_task',
        python_callable=slack_task_function,
        trigger_rule=TriggerRule.ALL_DONE
    )

    for dynamic_task in dynamic_tasks:
        dynamic_task >> slack_message_task