from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
from bson.json_util import dumps as mongo_dumps
import pandas as pd
import json
import oci
from public_functions.slack_integration import send_slack_message

webhook_url_engineer = Variable.get("WEBHOOK_URL_ENGINEER")
def task_failure_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg=f"{context['ti'].dag_id}")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_engineer, slack_msg="ETL Suitability OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")



default_args = {
    "owner": "oci",
}

with DAG(
    "etl_oci_onb_suitability",
    default_args=default_args,
    description="Load onb_suitability in oci",
    schedule_interval="0 10,14,18,22 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["oci", "mongodb", "onb_suitability"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract():        
        uri = Variable.get("MONGO_URL_PRD")


        client = MongoClient(uri)
        db = client["onboarding"]
        collection = db["onb_suitability"]
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)

        id = []
        score = []
        documentnumber = []
        profileId = []
        profile = []
        version = []
        answer_questionId_0 = []
        answer_answerScore_0 = []
        answer_questionId_1 = []
        answer_answerScore_1 = []
        answer_questionId_2 = []
        answer_answerScore_2 = []
        answer_questionId_3 = []
        answer_answerScore_3 = []
        answer_questionId_4 = []
        answer_answerScore_4 = []
        answer_questionId_5 = []
        answer_answerScore_5 = []
        answer_questionId_6 = []
        answer_answerScore_6 = []
        answer_questionId_7 = []
        answer_answerScore_7 = []
        answer_questionId_8 = []
        answer_answerScore_8 = []
        answer_questionId_9 = []
        answer_answerScore_9 = []
        answer_questionId_10 = []
        answer_answerScore_10 = []
        externalSource = []
        updatedat = []
        externalids_picpay = []
        expirationDate = []
        submissiondate = []
        createdat = []
        suitabilityId = []
        v = []


        def verifica_valor(valor, *args):
            if len(args) == 1:
                try:
                    return valor[args[0]]
                except:
                    return "null"
                logging.getLogger().info("try1")
            elif len(args) == 2:
                try:
                    return valor[args[0]][args[1]]
                except:
                    return "null"
                logging.getLogger().info("try2")
            elif len(args) == 3:
                try:
                    return valor[args[0]][args[1]][args[2]]
                except:
                    return "null"
                logging.getLogger().info("try3")
            elif len(args) == 4:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]]
                except:
                    return "null"


        for i in range(0, len(r)):
            id.append(verifica_valor(r[i], "_id", "$oid"))
            score.append(verifica_valor(r[i], "score"))
            documentnumber.append(verifica_valor(r[i], "documentNumber"))
            profileId.append(verifica_valor(r[i], "profileId"))
            profile.append(verifica_valor(r[i], "profile"))
            version.append(verifica_valor(r[i], "version"))
            answer_questionId_0.append(verifica_valor(r[i], "answer", 0, "questionId"))
            answer_answerScore_0.append(verifica_valor(r[i], "answer", 0, "answerScore"))
            answer_questionId_1.append(verifica_valor(r[i], "answer", 1, "questionId"))
            answer_answerScore_1.append(verifica_valor(r[i], "answer", 1, "answerScore"))
            answer_questionId_2.append(verifica_valor(r[i], "answer", 2, "questionId"))
            answer_answerScore_2.append(verifica_valor(r[i], "answer", 2, "answerScore"))
            answer_questionId_3.append(verifica_valor(r[i], "answer", 3, "questionId"))
            answer_answerScore_3.append(verifica_valor(r[i], "answer", 3, "answerScore"))
            answer_questionId_4.append(verifica_valor(r[i], "answer", 4, "questionId"))
            answer_answerScore_4.append(verifica_valor(r[i], "answer", 4, "answerScore"))
            answer_questionId_5.append(verifica_valor(r[i], "answer", 5, "questionId"))
            answer_answerScore_5.append(verifica_valor(r[i], "answer", 5, "answerScore"))
            answer_questionId_6.append(verifica_valor(r[i], "answer", 6, "questionId"))
            answer_answerScore_6.append(verifica_valor(r[i], "answer", 6, "answerScore"))
            answer_questionId_7.append(verifica_valor(r[i], "answer", 7, "questionId"))
            answer_answerScore_7.append(verifica_valor(r[i], "answer", 7, "answerScore"))
            answer_questionId_8.append(verifica_valor(r[i], "answer", 8, "questionId"))
            answer_answerScore_8.append(verifica_valor(r[i], "answer", 8, "answerScore"))
            answer_questionId_9.append(verifica_valor(r[i], "answer", 9, "questionId"))
            answer_answerScore_9.append(verifica_valor(r[i], "answer", 9, "answerScore"))
            answer_questionId_10.append(verifica_valor(r[i], "answer", 10, "questionId"))
            answer_answerScore_10.append(verifica_valor(r[i], "answer", 10, "answerScore"))
            externalSource.append(verifica_valor(r[i], "externalSource"))
            updatedat.append(verifica_valor(r[i], "updatedAt", "$date"))
            expirationDate.append(verifica_valor(r[i], "externalIds", "$date", "$numberLong"))
            externalids_picpay.append(verifica_valor(r[i], "expirationDate", "PICPAY"))
            submissiondate.append(verifica_valor(r[i], "submissionDate"))
            createdat.append(verifica_valor(r[i], "createdAt", "$date"))
            suitabilityId.append(verifica_valor(r[i], "suitabilityId"))
            v.append(verifica_valor(r[i], "__v"))


        df = pd.DataFrame(
        {
        "id": id,
        "score": score,
        "documentnumber": documentnumber,
        "profileid": profileId,
        "profile": profile,
        "version": version,
        "answer_questionId_0": answer_questionId_0,
        "answer_answerScore_0": answer_answerScore_0,
        "answer_questionId_1": answer_questionId_1,
        "answer_answerScore_1": answer_answerScore_1,
        "answer_questionId_2": answer_questionId_2,
        "answer_answerScore_2": answer_answerScore_2,
        "answer_questionId_3": answer_questionId_3,
        "answer_answerScore_3": answer_answerScore_3,
        "answer_questionId_4": answer_questionId_4,
        "answer_answerScore_4": answer_answerScore_4,
        "answer_questionId_5": answer_questionId_5,
        "answer_answerScore_5": answer_answerScore_5,
        "answer_questionId_6": answer_questionId_6,
        "answer_answerScore_6": answer_answerScore_6,
        "answer_questionId_7": answer_questionId_7,
        "answer_answerScore_7": answer_answerScore_7,
        "answer_questionId_8": answer_questionId_8,
        "answer_answerScore_8": answer_answerScore_8,
        "answer_questionId_9": answer_questionId_9,
        "answer_answerScore_9": answer_answerScore_9,
        "answer_questionId_10": answer_questionId_10,
        "answer_answerScore_10": answer_answerScore_10,
        "externalSource": externalSource,
        "updatedat": updatedat,
        "expirationDate": expirationDate,
        "externalids_picpay": externalids_picpay,
        "submissiondate": submissiondate,
        "createdat": createdat,
        "suitabilityId": suitabilityId,
        "v": v,
        }
        )
        logging.info(df)
        return df

    def create_pem_file():
        variable = Variable.get("KEY_OCI_PASSWORD")
        f = open(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem", "w")
        f.write(variable)
        f.close()

    @task()
    def load(df):
        create_pem_file()
        config = {
        "user": "ocid1.user.oc1..aaaaaaaaflq5zmwnfyrxdcuaiwfvwgimb7bqgcje7fls475q34zes7c775rq",
        "fingerprint": "fc:85:1d:fc:c7:84:2b:a9:c5:d9:c8:bd:ed:ca:68:ca",
        "tenancy": "ocid1.tenancy.oc1..aaaaaaaampaxw5fnhyel3vfyl4puke4mkatmpusviem6vycgj43o3so6g5ua",
        "region": "sa-vinhedo-1",
        "key_file": os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"
        }
        logging.info('Inicio Load')
        df.to_csv("oci://bkt-prod-veritas-silver@grilxlpa4rlr/MONGODB/MONGODB_ONB_SUITABILITY/onb_suitability.csv",sep=",",index=False,storage_options={"config": config},)
        logging.info("Enviado")

        logging.info("Deleção do arquivo .pem")
        if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"):
            os.remove(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem")
            logging.info(".pem deletado")
        else:
            print("Arquivo inexistente")

    load(extract())
