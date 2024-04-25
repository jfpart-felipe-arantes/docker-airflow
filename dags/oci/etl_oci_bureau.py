from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from logging import info
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.json_util import dumps as mongo_dumps
import pandas as pd
import json
from public_functions.slack_integration import send_slack_message
from sqlalchemy import create_engine
import pendulum
import pytz

local_tz = pendulum.timezone("America/Sao_Paulo")
timezone = pytz.timezone("America/Sao_Paulo")

import warnings
warnings.filterwarnings("ignore")

webhook_url_analytics = Variable.get("WEBHOOK_URL_ANALYTICS")
def task_failure_alert(context):
    send_slack_message(webhook_url_analytics, slack_msg=f"Bureau Failed")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_analytics, slack_msg="Bureau OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")

default_args = {
    "owner": "oci",
}

adw_table = "MONGODB_BUREAU"

with DAG(
    "etl_oci_bureau",
    default_args=default_args,
    description="Load bureau in oci",
    schedule_interval="0 2 * * 1-5",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["oci", "mongodb", "bureau"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract():        
        uri = Variable.get("MONGO_URL_PRD")

        client = MongoClient(uri)
        db = client["lionx"]
        collection = db["bureau"]
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)

        id = []
        cpf = []
        uniqueid = []
        request_date = []
        transaction_id = []
        requestId = []
        bureau_response_status = []
        type = []
        images = []
        sections_pfData_statusCode = []
        metadata_unique_id = []
        metadata_tenantId = []
        metadata_origin = []
        metadata_templateOrigin = []
        templateId = []
        createdAt = []
        fraud = []
        bureau_response_id = []
        statusReasons_category_0 = []
        statusReasons_code_0 = []
        statusReasons_status_0 = []
        statusReasons_resultStatus_0 = []
        statusReasons_category_1 = []
        statusReasons_code_1 = []
        statusReasons_status_1 = []
        statusReasons_resultStatus_1 = []
        statusReasons_category_2 = []
        statusReasons_code_2 = []
        statusReasons_status_2 = []
        statusReasons_resultStatus_2 = []
        statusReasons_category_3 = []
        statusReasons_code_3 = []
        statusReasons_status_3 = []
        statusReasons_resultStatus_3 = []
        statusReasons_category_4 = []
        statusReasons_code_4 = []
        statusReasons_status_4 = []
        statusReasons_resultStatus_4 = []
        attributes_cpf = []
        event_type = []
        status = []
        last_update_date = []
        status_reasons_category_0 = []
        status_reasons_code_0 = []
        status_reasons_status_0 = []
        status_reasons_resultStatus_0 = []
        status_reasons_description_0 = []
        status_reasons_category_1 = []
        status_reasons_code_1 = []
        status_reasons_status_1 = []
        status_reasons_resultStatus_1 = []
        status_reasons_description_1 = []
        status_reasons_category_2 = []
        status_reasons_code_2 = []
        status_reasons_status_2 = []
        status_reasons_resultStatus_2 = []
        status_reasons_description_2 = []
        status_reasons_category_3 = []
        status_reasons_code_3 = []
        status_reasons_status_3 = []
        status_reasons_resultStatus_3 = []
        status_reasons_description_3 = []
        status_reasons_category_4 = []
        status_reasons_code_4 = []
        status_reasons_status_4 = []
        status_reasons_resultStatus_4 = []
        status_reasons_description_4 = []
        status_reasons_category_5 = []
        status_reasons_code_5 = []
        status_reasons_status_5 = []
        status_reasons_resultStatus_5 = []
        status_reasons_description_5 = []
        validation_step = []
        softon_governmentInvolvement_0 = []
        softon_involvement_0 = []
        softon_newsDate_0 = []
        softon_id_0 = []
        softon_source_0 = []
        softon_existingProcess_0 = []
        softon_suspicionType_0 = []
        softon_name_0 = []
        softon_newsDtecLink_0 = []
        softon_governmentInvolvement_1 = []
        softon_involvement_1 = []
        softon_newsDate_1 = []
        softon_id_1 = []
        softon_source_1 = []
        softon_existingProcess_1 = []
        softon_suspicionType_1 = []
        softon_name_1 = []
        softon_newsDtecLink_1 = []
        softon_governmentInvolvement_2 = []
        softon_involvement_2 = []
        softon_newsDate_2 = []
        softon_id_2 = []
        softon_source_2 = []
        softon_existingProcess_2 = []
        softon_suspicionType_2 = []
        softon_name_2 = []
        softon_newsDtecLink_2 = []
        softon_governmentInvolvement_3 = []
        softon_involvement_3 = []
        softon_newsDate_3 = []
        softon_id_3 = []
        softon_source_3 = []
        softon_existingProcess_3 = []
        softon_suspicionType_3 = []
        softon_name_3 = []
        softon_newsDtecLink_3 = []
        softon_governmentInvolvement_4 = []
        softon_involvement_4 = []
        softon_newsDate_4 = []
        softon_id_4 = []
        softon_source_4 = []
        softon_existingProcess_4 = []
        softon_suspicionType_4 = []
        softon_name_4 = []
        softon_newsDtecLink_4 = []
        pfMbsArcaMs2_statusCode = []
        pfMbsArcaMs2_message = []
        pfMbsArcaMs2_code = []
        pfMbsArcaMs2_score = []
        pfMbsArcaMs2_status = []
        pfMbsArcaMs2_scorePLD = []
        pfMbsArcaMs2_requestId = []
        pfMbsArcaMs2_startAt = []
        pfMbsArcaMs2_endAt = []
        pfBiometriaUnico_statusCode = []
        pfBiometriaUnico_message = []
        pfBiometriaUnico_score = []
        pfBiometriaUnico_errorCode = []
        pfBiometriaUnico_startAt = []
        pfBiometriaUnico_endAt = []
        pfData_statusCode = []
        pfData_name = []
        pfData_taxIdStatus = []
        pfData_birthDate = []
        pfData_issueDate = []
        pfData_name = []
        ph3a_contactId = []
        ph3a_sequencialId = []
        ph3a_flags = []
        ph3a_flagsList_0 = []
        ph3a_flagsList_1 = []
        ph3a_flagsList_2 = []
        ph3a_flagsList_3 = []
        ph3a_name = []
        ph3a_gender = []
        ph3a_document = []
        ph3a_documentType = []
        ph3a_ranking = []
        ph3a_nameBrasil = []
        ph3a_birthDate = []
        ph3a_person_dependents = []
        ph3a_person_nationality = []
        ph3a_person_educationLevel = []
        ph3a_person_motherName = []
        ph3a_person_fatherName = []
        ph3a_creditScore_createDate = []
        ph3a_creditScore_status = []
        ph3a_phones_areaCode_0 = []
        ph3a_phones_number_0 = []
        ph3a_phones_formattedNumber_0 = []
        ph3a_phones_operatorId_0 = []
        ph3a_phones_operator_0 = []
        ph3a_phones_ranking_0 = []
        ph3a_phones_createDate_0 = []
        ph3a_phones_status_0 = []
        ph3a_phones_areaCode_1 = []
        ph3a_phones_number_1 = []
        ph3a_phones_formattedNumber_1 = []
        ph3a_phones_operatorId_1 = []
        ph3a_phones_operator_1 = []
        ph3a_phones_ranking_1 = []
        ph3a_phones_createDate_1 = []
        ph3a_phones_status_1 = []
        ph3a_phones_areaCode_2 = []
        ph3a_phones_number_2 = []
        ph3a_phones_formattedNumber_2 = []
        ph3a_phones_operatorId_2 = []
        ph3a_phones_operator_2 = []
        ph3a_phones_ranking_2 = []
        ph3a_phones_createDate_2 = []
        ph3a_phones_status_2 = []
        ph3a_emails_emails_0 = []
        ph3a_emails_createDate_0 = []
        ph3a_emails_status_0 = []
        ph3a_emails_emails_1 = []
        ph3a_emails_createDate_1 = []
        ph3a_emails_status_1 = []
        ph3a_addresses_street_0 = []
        ph3a_addresses_number_0 = []
        ph3a_addresses_district_0 = []
        ph3a_addresses_zipCode_0 = []
        ph3a_addresses_city_0 = []
        ph3a_addresses_state_0 = []
        ph3a_addresses_censusSector_0 = []
        ph3a_addresses_alias_0 = []
        ph3a_addresses_shortAlias_0 = []
        ph3a_addresses_score_0 = []
        ph3a_addresses_ranking_0 = []
        ph3a_addresses_createDate_0 = []
        ph3a_addresses_status_0 = []
        ph3a_addresses_street_1 = []
        ph3a_addresses_number_1 = []
        ph3a_addresses_district_1 = []
        ph3a_addresses_zipCode_1 = []
        ph3a_addresses_city_1 = []
        ph3a_addresses_state_1 = []
        ph3a_addresses_censusSector_1 = []
        ph3a_addresses_alias_1 = []
        ph3a_addresses_shortAlias_1 = []
        ph3a_addresses_score_1 = []
        ph3a_addresses_ranking_1 = []
        ph3a_addresses_createDate_1 = []
        ph3a_addresses_status_1 = []
        ph3a_addresses_street_2 = []
        ph3a_addresses_number_2 = []
        ph3a_addresses_district_2 = []
        ph3a_addresses_zipCode_2 = []
        ph3a_addresses_city_2 = []
        ph3a_addresses_state_2 = []
        ph3a_addresses_censusSector_2 = []
        ph3a_addresses_alias_2 = []
        ph3a_addresses_shortAlias_2 = []
        ph3a_addresses_score_2 = []
        ph3a_addresses_ranking_2 = []
        ph3a_addresses_createDate_2 = []
        ph3a_addresses_status_2 = []
        emailAge_email = []
        emailAge_ipAddress = []
        emailAge_eName = []
        emailAge_emailCreationDays = []
        emailAge_domainAge = []
        emailAge_domainCreationDays = []
        emailAge_firstVerificationDate = []
        emailAge_firstSeenDays = []
        emailAge_lastVerificationDate = []
        emailAge_status = []
        emailAge_fraudRisk = []
        emailAge_EAScore = []
        emailAge_EAAdvice = []
        emailAge_EARiskBandID = []
        emailAge_EARiskBand = []
        emailAge_domainName = []
        emailAge_domainCompany = []
        emailAge_domainCountryName = []
        emailAge_domainCategory = []
        emailAge_domainRiskLevel = []
        emailAge_domainRelevantInfo = []
        emailAge_domainRiskLevelID = []
        emailAge_domainRelevantInfoID = []
        emailAge_domainRiskCountry = []
        smLinks_ipRiskLevel = []
        smLinks_ipRiskReasonId = []
        smLinks_ipRiskReason = []
        smLinks_ipReputation = []
        smLinks_ipRiskLevelId = []
        smLinks_ipLatitude = []
        smLinks_ipLongitude = []
        smLinks_phoneStatus = []
        smLinks_deviceIdRiskLevel = []
        smLinks_overallDigitalIdentityScore = []
        smLinks_emailAge = []
        smLinks_fraudType = []
        smLinks_lastFlaggedon = []
        smLinks_emailExists = []
        smLinks_disDescription = []


        def verifica_valor(valor, *args):
            if len(args) == 1:
                try:
                    return valor[args[0]]
                except:
                    return None
            elif len(args) == 2:
                try:
                    return valor[args[0]][args[1]]
                except:
                    return None
            elif len(args) == 3:
                try:
                    return valor[args[0]][args[1]][args[2]]
                except:
                    return None
            elif len(args) == 4:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]]
                except:
                    return None
            elif len(args) == 5:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]][args[4]]
                except:
                    return None
            elif len(args) == 6:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]][args[4]][args[5]]
                except:
                    return None
            elif len(args) == 7:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]][args[4]][args[5]][args[6]]
                except:
                    return None
            elif len(args) == 8:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]][args[4]][args[5]][args[6]][args[7]]
                except:
                    return None
            elif len(args) == 9:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]][args[4]][args[5]][args[6]][args[7]][
                        args[8]
                    ]
                except:
                    return None
            elif len(args) == 10:
                try:
                    return valor[args[0]][args[1]][args[2]][args[3]][args[4]][args[5]][args[6]][args[7]][
                        args[8]
                    ][args[9]]
                except:
                    return None


        for i in range(0, len(r)):
            id.append(verifica_valor(r[i],'_id','$oid'))
            uniqueid.append(verifica_valor(r[i],'unique_id'))
            request_date.append(verifica_valor(r[i],'request_date'))
            transaction_id.append(verifica_valor(r[i],'transaction_id'))
            cpf.append(verifica_valor(r[i],'cpf'))
            requestId.append(verifica_valor(r[i],'bureau_response','requestId'))
            bureau_response_status.append(verifica_valor(r[i],'bureau_response','status'))
            type.append(verifica_valor(r[i],'bureau_response','type'))
            images.append(verifica_valor(r[i],'bureau_response','images'))
            sections_pfData_statusCode.append(verifica_valor(r[i],'bureau_response','sections','pfData','statusCode'))
            metadata_unique_id.append(verifica_valor(r[i],'bureau_response','metadata','unique_id'))
            metadata_tenantId.append(verifica_valor(r[i],'bureau_response','metadata','tenantId'))
            metadata_origin.append(verifica_valor(r[i],'bureau_response','metadata','origin'))
            metadata_templateOrigin.append(verifica_valor(r[i],'bureau_response','metadata','templateOrigin'))
            templateId.append(verifica_valor(r[i],'bureau_response','templateId'))
            createdAt.append(verifica_valor(r[i],'bureau_response','createdAt'))
            fraud.append(verifica_valor(r[i],'bureau_response','fraud'))
            bureau_response_id.append(verifica_valor(r[i],'bureau_response','id'))
            statusReasons_category_0.append(verifica_valor(r[i],'bureau_response','statusReasons',0,'category'))
            statusReasons_code_0.append(verifica_valor(r[i],'bureau_response','statusReasons',0,'code'))
            statusReasons_status_0.append(verifica_valor(r[i],'bureau_response','statusReasons',0,'status'))
            statusReasons_resultStatus_0.append(verifica_valor(r[i],'bureau_response','statusReasons',0,'resultStatus'))
            statusReasons_category_1.append(verifica_valor(r[i],'bureau_response','statusReasons',1,'category'))
            statusReasons_code_1.append(verifica_valor(r[i],'bureau_response','statusReasons',1,'code'))
            statusReasons_status_1.append(verifica_valor(r[i],'bureau_response','statusReasons',1,'status'))
            statusReasons_resultStatus_1.append(verifica_valor(r[i],'bureau_response','statusReasons',1,'resultStatus'))
            statusReasons_category_2.append(verifica_valor(r[i],'bureau_response','statusReasons',2,'category'))
            statusReasons_code_2.append(verifica_valor(r[i],'bureau_response','statusReasons',2,'code'))
            statusReasons_status_2.append(verifica_valor(r[i],'bureau_response','statusReasons',2,'status'))
            statusReasons_resultStatus_2.append(verifica_valor(r[i],'bureau_response','statusReasons',2,'resultStatus'))
            statusReasons_category_3.append(verifica_valor(r[i],'bureau_response','statusReasons',3,'category'))
            statusReasons_code_3.append(verifica_valor(r[i],'bureau_response','statusReasons',3,'code'))
            statusReasons_status_3.append(verifica_valor(r[i],'bureau_response','statusReasons',3,'status'))
            statusReasons_resultStatus_3.append(verifica_valor(r[i],'bureau_response','statusReasons',3,'resultStatus'))
            statusReasons_category_4.append(verifica_valor(r[i],'bureau_response','statusReasons',4,'category'))
            statusReasons_code_4.append(verifica_valor(r[i],'bureau_response','statusReasons',4,'code'))
            statusReasons_status_4.append(verifica_valor(r[i],'bureau_response','statusReasons',4,'status'))
            statusReasons_resultStatus_4.append(verifica_valor(r[i],'bureau_response','statusReasons',4,'resultStatus'))
            attributes_cpf.append(verifica_valor(r[i],'bureau_response','attributes','cpf'))
            event_type.append(verifica_valor(r[i],'event_type'))
            status.append(verifica_valor(r[i],'status'))
            last_update_date.append(verifica_valor(r[i],'last_update_date'))
            status_reasons_category_0.append(verifica_valor(r[i],'status_reasons',0,'category'))
            status_reasons_code_0.append(verifica_valor(r[i],'status_reasons',0,'code'))
            status_reasons_status_0.append(verifica_valor(r[i],'status_reasons',0,'status'))
            status_reasons_resultStatus_0.append(verifica_valor(r[i],'status_reasons',0,'resultStatus'))
            status_reasons_description_0.append(verifica_valor(r[i],'status_reasons',0,'description'))
            status_reasons_category_1.append(verifica_valor(r[i],'status_reasons',1,'category'))
            status_reasons_code_1.append(verifica_valor(r[i],'status_reasons',1,'code'))
            status_reasons_status_1.append(verifica_valor(r[i],'status_reasons',1,'status'))
            status_reasons_resultStatus_1.append(verifica_valor(r[i],'status_reasons',1,'resultStatus'))
            status_reasons_description_1.append(verifica_valor(r[i],'status_reasons',1,'description'))
            status_reasons_category_2.append(verifica_valor(r[i],'status_reasons',2,'category'))
            status_reasons_code_2.append(verifica_valor(r[i],'status_reasons',2,'code'))
            status_reasons_status_2.append(verifica_valor(r[i],'status_reasons',2,'status'))
            status_reasons_resultStatus_2.append(verifica_valor(r[i],'status_reasons',2,'resultStatus'))
            status_reasons_description_2.append(verifica_valor(r[i],'status_reasons',2,'description'))
            status_reasons_category_3.append(verifica_valor(r[i],'status_reasons',3,'category'))
            status_reasons_code_3.append(verifica_valor(r[i],'status_reasons',3,'code'))
            status_reasons_status_3.append(verifica_valor(r[i],'status_reasons',3,'status'))
            status_reasons_resultStatus_3.append(verifica_valor(r[i],'status_reasons',3,'resultStatus'))
            status_reasons_description_3.append(verifica_valor(r[i],'status_reasons',3,'description'))
            status_reasons_category_4.append(verifica_valor(r[i],'status_reasons',4,'category'))
            status_reasons_code_4.append(verifica_valor(r[i],'status_reasons',4,'code'))
            status_reasons_status_4.append(verifica_valor(r[i],'status_reasons',4,'status'))
            status_reasons_resultStatus_4.append(verifica_valor(r[i],'status_reasons',4,'resultStatus'))
            status_reasons_description_4.append(verifica_valor(r[i],'status_reasons',4,'description'))
            status_reasons_category_5.append(verifica_valor(r[i],'status_reasons',5,'category'))
            status_reasons_code_5.append(verifica_valor(r[i],'status_reasons',5,'code'))
            status_reasons_status_5.append(verifica_valor(r[i],'status_reasons',5,'status'))
            status_reasons_resultStatus_5.append(verifica_valor(r[i],'status_reasons',5,'resultStatus'))
            status_reasons_description_5.append(verifica_valor(r[i],'status_reasons',5,'description'))
            validation_step.append(verifica_valor(r[i],'validation_step'))
            softon_governmentInvolvement_0.append(verifica_valor(r[i],'softon',0,'governmentInvolvement'))
            softon_involvement_0.append(verifica_valor(r[i],'softon',0,'involvement'))
            softon_newsDate_0.append(verifica_valor(r[i],'softon',0,'newsDate'))
            softon_id_0.append(verifica_valor(r[i],'softon',0,'id'))
            softon_source_0.append(verifica_valor(r[i],'softon',0,'source'))
            softon_existingProcess_0.append(verifica_valor(r[i],'softon',0,'existingProcess'))
            softon_suspicionType_0.append(verifica_valor(r[i],'softon',0,'suspicionType'))
            softon_name_0.append(verifica_valor(r[i],'softon',0,'name'))
            softon_newsDtecLink_0.append(verifica_valor(r[i],'softon',0,'newsDtecLink'))
            softon_governmentInvolvement_1.append(verifica_valor(r[i],'softon',1,'governmentInvolvement'))
            softon_involvement_1.append(verifica_valor(r[i],'softon',1,'involvement'))
            softon_newsDate_1.append(verifica_valor(r[i],'softon',1,'newsDate'))
            softon_id_1.append(verifica_valor(r[i],'softon',1,'id'))
            softon_source_1.append(verifica_valor(r[i],'softon',1,'source'))
            softon_existingProcess_1.append(verifica_valor(r[i],'softon',1,'existingProcess'))
            softon_suspicionType_1.append(verifica_valor(r[i],'softon',1,'suspicionType'))
            softon_name_1.append(verifica_valor(r[i],'softon',1,'name'))
            softon_newsDtecLink_1.append(verifica_valor(r[i],'softon',1,'newsDtecLink'))
            softon_governmentInvolvement_2.append(verifica_valor(r[i],'softon',2,'governmentInvolvement'))
            softon_involvement_2.append(verifica_valor(r[i],'softon',2,'involvement'))
            softon_newsDate_2.append(verifica_valor(r[i],'softon',2,'newsDate'))
            softon_id_2.append(verifica_valor(r[i],'softon',2,'id'))
            softon_source_2.append(verifica_valor(r[i],'softon',2,'source'))
            softon_existingProcess_2.append(verifica_valor(r[i],'softon',2,'existingProcess'))
            softon_suspicionType_2.append(verifica_valor(r[i],'softon',2,'suspicionType'))
            softon_name_2.append(verifica_valor(r[i],'softon',2,'name'))
            softon_newsDtecLink_2.append(verifica_valor(r[i],'softon',2,'newsDtecLink'))
            softon_governmentInvolvement_3.append(verifica_valor(r[i],'softon',3,'governmentInvolvement'))
            softon_involvement_3.append(verifica_valor(r[i],'softon',3,'involvement'))
            softon_newsDate_3.append(verifica_valor(r[i],'softon',3,'newsDate'))
            softon_id_3.append(verifica_valor(r[i],'softon',3,'id'))
            softon_source_3.append(verifica_valor(r[i],'softon',3,'source'))
            softon_existingProcess_3.append(verifica_valor(r[i],'softon',3,'existingProcess'))
            softon_suspicionType_3.append(verifica_valor(r[i],'softon',3,'suspicionType'))
            softon_name_3.append(verifica_valor(r[i],'softon',3,'name'))
            softon_newsDtecLink_3.append(verifica_valor(r[i],'softon',3,'newsDtecLink'))
            softon_governmentInvolvement_4.append(verifica_valor(r[i],'softon',4,'governmentInvolvement'))
            softon_involvement_4.append(verifica_valor(r[i],'softon',4,'involvement'))
            softon_newsDate_4.append(verifica_valor(r[i],'softon',4,'newsDate'))
            softon_id_4.append(verifica_valor(r[i],'softon',4,'id'))
            softon_source_4.append(verifica_valor(r[i],'softon',4,'source'))
            softon_existingProcess_4.append(verifica_valor(r[i],'softon',4,'existingProcess'))
            softon_suspicionType_4.append(verifica_valor(r[i],'softon',4,'suspicionType'))
            softon_name_4.append(verifica_valor(r[i],'softon',4,'name'))
            softon_newsDtecLink_4.append(verifica_valor(r[i],'softon',4,'newsDtecLink'))
            pfMbsArcaMs2_statusCode.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','statusCode'))
            pfMbsArcaMs2_message.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','message'))
            pfMbsArcaMs2_code.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','code'))
            pfMbsArcaMs2_score.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','score'))
            pfMbsArcaMs2_status.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','status'))
            pfMbsArcaMs2_scorePLD.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','scorePLD'))
            pfMbsArcaMs2_requestId.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','requestId'))
            pfMbsArcaMs2_startAt.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','query','startAt'))
            pfMbsArcaMs2_endAt.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','query','endAt'))
            pfBiometriaUnico_statusCode.append(verifica_valor(r[i],'bureau_response','sections','pfBiometriaUnico','statusCode'))
            pfBiometriaUnico_message.append(verifica_valor(r[i],'bureau_response','sections','pfBiometriaUnico','data','message'))
            pfBiometriaUnico_score.append(verifica_valor(r[i],'bureau_response','sections','pfBiometriaUnico','data','score'))
            pfBiometriaUnico_errorCode.append(verifica_valor(r[i],'bureau_response','sections','pfBiometriaUnico','data','errorCode'))
            pfBiometriaUnico_startAt.append(verifica_valor(r[i],'bureau_response','sections','pfBiometriaUnico','query','startAt'))
            pfBiometriaUnico_endAt.append(verifica_valor(r[i],'bureau_response','sections','pfBiometriaUnico','query','endAt'))
            pfData_statusCode.append(verifica_valor(r[i],'bureau_response','sections','pfData','statusCode'))
            pfData_name.append(verifica_valor(r[i],'bureau_response','sections','pfData','data','name'))
            pfData_taxIdStatus.append(verifica_valor(r[i],'bureau_response','sections','pfData','data','taxIdStatus'))
            pfData_birthDate.append(verifica_valor(r[i],'bureau_response','sections','pfData','data','birthDate'))
            pfData_issueDate.append(verifica_valor(r[i],'bureau_response','sections','pfData','data','issueDate'))
            ph3a_contactId.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','contactId'))
            ph3a_sequencialId.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','sequencialId'))
            ph3a_flags.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','flags'))
            ph3a_flagsList_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','flagList','0'))
            ph3a_flagsList_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','flagList','1'))
            ph3a_flagsList_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','flagList','2'))
            ph3a_flagsList_3.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','flagList','3'))
            ph3a_name.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','name'))
            ph3a_gender.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','gender'))
            ph3a_document.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','document'))
            ph3a_documentType.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','documentType'))
            ph3a_ranking.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','ranking'))
            ph3a_nameBrasil.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','nameBrasil'))
            ph3a_birthDate.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','birthDate'))
            ph3a_person_dependents.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','person','dependents'))
            ph3a_person_nationality.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','person','nationality'))
            ph3a_person_educationLevel.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','person','educationLevel'))
            ph3a_person_motherName.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','person','motherName'))
            ph3a_person_fatherName.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','person','fatherName'))
            ph3a_creditScore_createDate.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','creditScore','createDate'))
            ph3a_creditScore_status.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','creditScore','status'))
            ph3a_phones_areaCode_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'areaCode'))
            ph3a_phones_number_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'number'))
            ph3a_phones_formattedNumber_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'formattedNumber'))
            ph3a_phones_operatorId_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'operatorId'))
            ph3a_phones_operator_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'operator'))
            ph3a_phones_ranking_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'ranking'))
            ph3a_phones_createDate_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'createDate'))
            ph3a_phones_status_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',0,'status'))
            ph3a_phones_areaCode_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'areaCode'))
            ph3a_phones_number_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'number'))
            ph3a_phones_formattedNumber_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'formattedNumber'))
            ph3a_phones_operatorId_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'operatorId'))
            ph3a_phones_operator_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'operator'))
            ph3a_phones_ranking_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'ranking'))
            ph3a_phones_createDate_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'createDate'))
            ph3a_phones_status_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',1,'status'))
            ph3a_phones_areaCode_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'areaCode'))
            ph3a_phones_number_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'number'))
            ph3a_phones_formattedNumber_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'formattedNumber'))
            ph3a_phones_operatorId_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'operatorId'))
            ph3a_phones_operator_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'operator'))
            ph3a_phones_ranking_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'ranking'))
            ph3a_phones_createDate_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'createDate'))
            ph3a_phones_status_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','phones',2,'status'))
            ph3a_emails_emails_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','emails',0,'email'))
            ph3a_emails_createDate_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','emails',0,'createDate'))
            ph3a_emails_status_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','emails',0,'status'))
            ph3a_emails_emails_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','emails',1,'email'))
            ph3a_emails_createDate_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','emails',1,'createDate'))
            ph3a_emails_status_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','emails',1,'status'))
            ph3a_addresses_street_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'street'))
            ph3a_addresses_number_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'number'))
            ph3a_addresses_district_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'district'))
            ph3a_addresses_zipCode_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'zipCode'))
            ph3a_addresses_city_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'city'))
            ph3a_addresses_state_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'state'))
            ph3a_addresses_censusSector_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'censusSector'))
            ph3a_addresses_alias_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'alias'))
            ph3a_addresses_shortAlias_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'shortAlias'))
            ph3a_addresses_score_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'score'))
            ph3a_addresses_ranking_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'ranking'))
            ph3a_addresses_createDate_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'createDate'))
            ph3a_addresses_status_0.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',0,'status'))
            ph3a_addresses_street_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'street'))
            ph3a_addresses_number_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'number'))
            ph3a_addresses_district_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'district'))
            ph3a_addresses_zipCode_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'zipCode'))
            ph3a_addresses_city_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'city'))
            ph3a_addresses_state_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'state'))
            ph3a_addresses_censusSector_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'censusSector'))
            ph3a_addresses_alias_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'alias'))
            ph3a_addresses_shortAlias_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'shortAlias'))
            ph3a_addresses_score_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'score'))
            ph3a_addresses_ranking_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'ranking'))
            ph3a_addresses_createDate_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'createDate'))
            ph3a_addresses_status_1.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',1,'status'))
            ph3a_addresses_street_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'street'))
            ph3a_addresses_number_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'number'))
            ph3a_addresses_district_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'district'))
            ph3a_addresses_zipCode_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'zipCode'))
            ph3a_addresses_city_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'city'))
            ph3a_addresses_state_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'state'))
            ph3a_addresses_censusSector_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'censusSector'))
            ph3a_addresses_alias_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'alias'))
            ph3a_addresses_shortAlias_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'shortAlias'))
            ph3a_addresses_score_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'score'))
            ph3a_addresses_ranking_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'ranking'))
            ph3a_addresses_createDate_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'createDate'))
            ph3a_addresses_status_2.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','ph3a','addresses',2,'status'))
            emailAge_email.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'email'))
            emailAge_ipAddress.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipAddress'))
            emailAge_eName.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'eName'))
            emailAge_emailCreationDays.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'emailCreationDays'))
            emailAge_domainAge.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainAge'))
            emailAge_domainCreationDays.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainCreationDays'))
            emailAge_firstVerificationDate.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'firstVerificationDate'))
            emailAge_firstSeenDays.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'firstSeenDays'))
            emailAge_lastVerificationDate.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'lastVerificationDate'))
            emailAge_status.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'status'))
            emailAge_fraudRisk.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'fraudRisk'))
            emailAge_EAScore.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'EAScore'))
            emailAge_EAAdvice.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'EAAdvice'))
            emailAge_EARiskBandID.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'EARiskBandID'))
            emailAge_EARiskBand.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'EARiskBand'))
            emailAge_domainName.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainName'))
            emailAge_domainCompany.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainCompany'))
            emailAge_domainCountryName.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainCountryName'))
            emailAge_domainCategory.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainCategory'))
            emailAge_domainRiskLevel.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainRiskLevel'))
            emailAge_domainRelevantInfo.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainRelevantInfo'))
            emailAge_domainRiskLevelID.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainRiskLevelID'))
            emailAge_domainRelevantInfoID.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainRelevantInfoID'))
            emailAge_domainRiskCountry.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'domainRiskCountry'))
            smLinks_ipRiskLevel.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipRiskLevel'))
            smLinks_ipRiskReasonId.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipRiskReasonId'))
            smLinks_ipRiskReason.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipRiskReason'))
            smLinks_ipReputation.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipReputation'))
            smLinks_ipRiskLevelId.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipRiskLevelId'))
            smLinks_ipLatitude.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipLatitude'))
            smLinks_ipLongitude.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'ipLongitude'))
            smLinks_phoneStatus.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'phoneStatus'))
            smLinks_deviceIdRiskLevel.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'deviceIdRiskLevel'))
            smLinks_overallDigitalIdentityScore.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'overallDigitalIdentityScore'))
            smLinks_emailAge.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'emailAge'))
            smLinks_fraudType.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'fraudType'))
            smLinks_lastFlaggedon.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'lastFlaggedon'))
            smLinks_emailExists.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'emailExists'))
            smLinks_disDescription.append(verifica_valor(r[i],'bureau_response','sections','pfMbsArcaMs2','data','emailAge',0,'disDescription'))


        df = pd.DataFrame(
            {
                "id": id,
                "uniqueid": uniqueid,
                "request_date": request_date,
                "transaction_id": transaction_id,
                "cpf": cpf,
                "requestId": requestId,
                "bureau_response_status": bureau_response_status,
                "type": type,
                "images": images,
                "sections_pfData_statusCode": sections_pfData_statusCode,
                "metadata_unique_id": metadata_unique_id,
                "metadata_tenantId": metadata_tenantId,
                "metadata_origin": metadata_origin,
                "metadata_templateOrigin": metadata_templateOrigin,
                "templateId": templateId,
                "createdAt": createdAt,
                "fraud": fraud,
                "bureau_response_id": bureau_response_id,
                "statusReasons_category_0": statusReasons_category_0,
                "statusReasons_code_0": statusReasons_code_0,
                "statusReasons_status_0": statusReasons_status_0,
                "statusReasons_resultStatus_0": statusReasons_resultStatus_0,
                "statusReasons_category_1": statusReasons_category_1,
                "statusReasons_code_1": statusReasons_code_1,
                "statusReasons_status_1": statusReasons_status_1,
                "statusReasons_resultStatus_1": statusReasons_resultStatus_1,
                "statusReasons_category_2": statusReasons_category_2,
                "statusReasons_code_2": statusReasons_code_2,
                "statusReasons_status_2": statusReasons_status_2,
                "statusReasons_resultStatus_2": statusReasons_resultStatus_2,
                "statusReasons_category_3": statusReasons_category_3,
                "statusReasons_code_3": statusReasons_code_3,
                "statusReasons_status_3": statusReasons_status_3,
                "statusReasons_resultStatus_3": statusReasons_resultStatus_3,
                "statusReasons_category_4": statusReasons_category_4,
                "statusReasons_code_4": statusReasons_code_4,
                "statusReasons_status_4": statusReasons_status_4,
                "statusReasons_resultStatus_4": statusReasons_resultStatus_4,
                "attributes_cpf": attributes_cpf,
                "event_type": event_type,
                "status": status,
                "status_reasons_category_0": status_reasons_category_0,
                "status_reasons_code_0": status_reasons_code_0,
                "status_reasons_status_0 ": status_reasons_status_0,
                "status_reasons_resultStatus_0": status_reasons_resultStatus_0,
                "status_reasons_description_0": status_reasons_description_0,
                "status_reasons_category_1": status_reasons_category_1,
                "status_reasons_code_1": status_reasons_code_1,
                "status_reasons_status_1": status_reasons_status_1,
                "status_reasons_resultStatus_1": status_reasons_resultStatus_1,
                "status_reasons_description_1": status_reasons_description_1,
                "status_reasons_category_2": status_reasons_category_2,
                "status_reasons_code_2": status_reasons_code_2,
                "status_reasons_status_2": status_reasons_status_2,
                "status_reasons_resultStatus_2": status_reasons_resultStatus_2,
                "status_reasons_description_2 ": status_reasons_description_2,
                "status_reasons_category_3": status_reasons_category_3,
                "status_reasons_code_3": status_reasons_code_3,
                "status_reasons_status_3": status_reasons_status_3,
                "status_reasons_resultStatus_3": status_reasons_resultStatus_3,
                "status_reasons_description_3": status_reasons_description_3,
                "status_reasons_category_4": status_reasons_category_4,
                "status_reasons_code_4": status_reasons_code_4,
                "status_reasons_status_4": status_reasons_status_4,
                "status_reasons_resultStatus_4": status_reasons_resultStatus_4,
                "status_reasons_description_4": status_reasons_description_4,
                "status_reasons_category_5": status_reasons_category_5,
                "status_reasons_code_5": status_reasons_code_5,
                "status_reasons_status_5": status_reasons_status_5,
                "status_reasons_resultStatus_5": status_reasons_resultStatus_5,
                "status_reasons_description_5": status_reasons_description_5,
                "validation_step": validation_step,
                "softon_governmentInvolvement_0": softon_governmentInvolvement_0,
                "softon_involvement_0": softon_involvement_0,
                "softon_newsDate_0": softon_newsDate_0,
                "softon_id_0": softon_id_0,
                "softon_source_0": softon_source_0,
                "softon_existingProcess_0": softon_existingProcess_0,
                "softon_suspicionType_0": softon_suspicionType_0,
                "softon_name_0": softon_name_0,
                "softon_newsDtecLink_0": softon_newsDtecLink_0,
                "softon_governmentInvolvement_1": softon_governmentInvolvement_1,
                "softon_involvement_1": softon_involvement_1,
                "softon_newsDate_1": softon_newsDate_1,
                "softon_id_1": softon_id_1,
                "softon_source_1": softon_source_1,
                "softon_existingProcess_1": softon_existingProcess_1,
                "softon_suspicionType_1": softon_suspicionType_1,
                "softon_name_1": softon_name_1,
                "softon_newsDtecLink_1": softon_newsDtecLink_1,
                "softon_governmentInvolvement_2": softon_governmentInvolvement_2,
                "softon_involvement_2": softon_involvement_2,
                "softon_newsDate_2": softon_newsDate_2,
                "softon_id_2": softon_id_2,
                "softon_source_2": softon_source_2,
                "softon_existingProcess_2": softon_existingProcess_2,
                "softon_suspicionType_2": softon_suspicionType_2,
                "softon_name_2": softon_name_2,
                "softon_newsDtecLink_2": softon_newsDtecLink_2,
                "softon_governmentInvolvement_3": softon_governmentInvolvement_3,
                "softon_involvement_3": softon_involvement_3,
                "softon_newsDate_3": softon_newsDate_3,
                "softon_id_3": softon_id_3,
                "softon_source_3": softon_source_3,
                "softon_existingProcess_3": softon_existingProcess_3,
                "softon_suspicionType_3": softon_suspicionType_3,
                "softon_name_3": softon_name_3,
                "softon_newsDtecLink_3": softon_newsDtecLink_3,
                "softon_governmentInvolvement_4": softon_governmentInvolvement_4,
                "softon_involvement_4": softon_involvement_4,
                "softon_newsDate_4": softon_newsDate_4,
                "softon_id_4": softon_id_4,
                "softon_source_4": softon_source_4,
                "softon_existingProcess_4": softon_existingProcess_4,
                "softon_suspicionType_4": softon_suspicionType_4,
                "softon_name_4": softon_name_4,
                "softon_newsDtecLink_4": softon_newsDtecLink_4,
                "pfMbsArcaMs2_statusCode": pfMbsArcaMs2_statusCode,
                "pfMbsArcaMs2_message": pfMbsArcaMs2_message,
                "pfMbsArcaMs2_code": pfMbsArcaMs2_code,
                "pfMbsArcaMs2_score": pfMbsArcaMs2_score,
                "pfMbsArcaMs2_status": pfMbsArcaMs2_status,
                "pfMbsArcaMs2_scorePLD": pfMbsArcaMs2_scorePLD,
                "pfMbsArcaMs2_requestId": pfMbsArcaMs2_requestId,
                "pfMbsArcaMs2_startAt": pfMbsArcaMs2_startAt,
                "pfMbsArcaMs2_endAt": pfMbsArcaMs2_endAt,
                "pfBiometriaUnico_statusCode": pfBiometriaUnico_statusCode,
                "pfBiometriaUnico_message": pfBiometriaUnico_message,
                "pfBiometriaUnico_score": pfBiometriaUnico_score,
                "pfBiometriaUnico_errorCode": pfBiometriaUnico_errorCode,
                "pfBiometriaUnico_startAt": pfBiometriaUnico_startAt,
                "pfBiometriaUnico_endAt": pfBiometriaUnico_endAt,
                "pfData_statusCode": pfData_statusCode,
                "pfData_name": pfData_name,
                "pfData_taxIdStatus": pfData_taxIdStatus,
                "pfData_birthDate": pfData_birthDate,
                "pfData_issueDate": pfData_issueDate,
                "pfData_name": pfData_name,
                "ph3a_contactId": ph3a_contactId,
                "ph3a_sequencialId": ph3a_sequencialId,
                "ph3a_flags": ph3a_flags,
                "ph3a_flagsList_0": ph3a_flagsList_0,
                "ph3a_flagsList_1": ph3a_flagsList_1,
                "ph3a_flagsList_2": ph3a_flagsList_2,
                "ph3a_flagsList_3": ph3a_flagsList_3,
                "ph3a_name": ph3a_name,
                "ph3a_gender": ph3a_gender,
                "ph3a_document": ph3a_document,
                "ph3a_documentType": ph3a_documentType,
                "ph3a_ranking": ph3a_ranking,
                "ph3a_nameBrasil": ph3a_nameBrasil,
                "ph3a_birthDate": ph3a_birthDate,
                "ph3a_person_dependents": ph3a_person_dependents,
                # "ph3a_person_nationality": ph3a_person_nationality,
                # "ph3a_person_educationLevel": ph3a_person_educationLevel,
                "ph3a_person_motherName": ph3a_person_motherName,
                "ph3a_person_fatherName": ph3a_person_fatherName,
                "ph3a_creditScore_createDate": ph3a_creditScore_createDate,
                # "ph3a_creditScore_status": ph3a_creditScore_status,
                "ph3a_phones_areaCode_0": ph3a_phones_areaCode_0,
                "ph3a_phones_number_0": ph3a_phones_number_0,
                "ph3a_phones_formattedNumber_0": ph3a_phones_formattedNumber_0,
                "ph3a_phones_operatorId_0": ph3a_phones_operatorId_0,
                "ph3a_phones_operator_0": ph3a_phones_operator_0,
                "ph3a_phones_ranking_0": ph3a_phones_ranking_0,
                "ph3a_phones_createDate_0": ph3a_phones_createDate_0,
                "ph3a_phones_status_0": ph3a_phones_status_0,
                "ph3a_phones_areaCode_1": ph3a_phones_areaCode_1,
                "ph3a_phones_number_1": ph3a_phones_number_1,
                "ph3a_phones_formattedNumber_1": ph3a_phones_formattedNumber_1,
                "ph3a_phones_operatorId_1": ph3a_phones_operatorId_1,
                "ph3a_phones_operator_1": ph3a_phones_operator_1,
                "ph3a_phones_ranking_1": ph3a_phones_ranking_1,
                "ph3a_phones_createDate_1": ph3a_phones_createDate_1,
                "ph3a_phones_status_1": ph3a_phones_status_1,
                "ph3a_phones_areaCode_2": ph3a_phones_areaCode_2,
                "ph3a_phones_number_2": ph3a_phones_number_2,
                "ph3a_phones_formattedNumber_2": ph3a_phones_formattedNumber_2,
                "ph3a_phones_operatorId_2": ph3a_phones_operatorId_2,
                "ph3a_phones_operator_2": ph3a_phones_operator_2,
                "ph3a_phones_ranking_2": ph3a_phones_ranking_2,
                "ph3a_phones_createDate_2": ph3a_phones_createDate_2,
                "ph3a_phones_status_2": ph3a_phones_status_2,
                "ph3a_emails_emails_0": ph3a_emails_emails_0,
                "ph3a_emails_createDate_0": ph3a_emails_createDate_0,
                "ph3a_emails_status_0": ph3a_emails_status_0,
                "ph3a_emails_emails_1": ph3a_emails_emails_1,
                "ph3a_emails_createDate_1": ph3a_emails_createDate_1,
                "ph3a_emails_status_1": ph3a_emails_status_1,
                "ph3a_addresses_street_0": ph3a_addresses_street_0,
                "ph3a_addresses_number_0": ph3a_addresses_number_0,
                "ph3a_addresses_district_0": ph3a_addresses_district_0,
                "ph3a_addresses_zipCode_0": ph3a_addresses_zipCode_0,
                "ph3a_addresses_city_0": ph3a_addresses_city_0,
                "ph3a_addresses_state_0": ph3a_addresses_state_0,
                "ph3a_addresses_censusSector_0": ph3a_addresses_censusSector_0,
                "ph3a_addresses_alias_0": ph3a_addresses_alias_0,
                "ph3a_addresses_shortAlias_0": ph3a_addresses_shortAlias_0,
                "ph3a_addresses_score_0": ph3a_addresses_score_0,
                "ph3a_addresses_ranking_0": ph3a_addresses_ranking_0,
                "ph3a_addresses_createDate_0": ph3a_addresses_createDate_0,
                "ph3a_addresses_status_0": ph3a_addresses_status_0,
                "ph3a_addresses_street_1": ph3a_addresses_street_1,
                "ph3a_addresses_number_1": ph3a_addresses_number_1,
                "ph3a_addresses_district_1": ph3a_addresses_district_1,
                "ph3a_addresses_zipCode_1": ph3a_addresses_zipCode_1,
                "ph3a_addresses_city_1": ph3a_addresses_city_1,
                "ph3a_addresses_state_1": ph3a_addresses_state_1,
                "ph3a_addresses_censusSector_1": ph3a_addresses_censusSector_1,
                "ph3a_addresses_alias_1": ph3a_addresses_alias_1,
                "ph3a_addresses_shortAlias_1": ph3a_addresses_shortAlias_1,
                "ph3a_addresses_score_1": ph3a_addresses_score_1,
                "ph3a_addresses_ranking_1": ph3a_addresses_ranking_1,
                "ph3a_addresses_createDate_1": ph3a_addresses_createDate_1,
                "ph3a_addresses_status_1": ph3a_addresses_status_1,
                "ph3a_addresses_street_2": ph3a_addresses_street_2,
                "ph3a_addresses_number_2": ph3a_addresses_number_2,
                "ph3a_addresses_district_2": ph3a_addresses_district_2,
                "ph3a_addresses_zipCode_2": ph3a_addresses_zipCode_2,
                "ph3a_addresses_city_2": ph3a_addresses_city_2,
                "ph3a_addresses_state_2": ph3a_addresses_state_2,
                "ph3a_addresses_censusSector_2": ph3a_addresses_censusSector_2,
                "ph3a_addresses_alias_2": ph3a_addresses_alias_2,
                "ph3a_addresses_shortAlias_2": ph3a_addresses_shortAlias_2,
                "ph3a_addresses_score_2": ph3a_addresses_score_2,
                "ph3a_addresses_ranking_2": ph3a_addresses_ranking_2,
                "ph3a_addresses_createDate_2": ph3a_addresses_createDate_2,
                "ph3a_addresses_status_2": ph3a_addresses_status_2,
                "emailAge_email": emailAge_email,
                "emailAge_ipAddress": emailAge_ipAddress,
                "emailAge_eName": emailAge_eName,
                "emailAge_emailCreationDays": emailAge_emailCreationDays,
                "emailAge_domainAge": emailAge_domainAge,
                "emailAge_domainCreationDays": emailAge_domainCreationDays,
                "emailAge_firstVerificationDate": emailAge_firstVerificationDate,
                "emailAge_firstSeenDays": emailAge_firstSeenDays,
                "emailAge_lastVerificationDate": emailAge_lastVerificationDate,
                "emailAge_status": emailAge_status,
                "emailAge_fraudRisk": emailAge_fraudRisk,
                "emailAge_EAScore": emailAge_EAScore,
                "emailAge_EAAdvice": emailAge_EAAdvice,
                "emailAge_EARiskBandID": emailAge_EARiskBandID,
                "emailAge_EARiskBand": emailAge_EARiskBand,
                "emailAge_domainName": emailAge_domainName,
                "emailAge_domainCompany": emailAge_domainCompany,
                "emailAge_domainCountryName": emailAge_domainCountryName,
                "emailAge_domainCategory": emailAge_domainCategory,
                "emailAge_domainRiskLevel": emailAge_domainRiskLevel,
                "emailAge_domainRelevantInfo": emailAge_domainRelevantInfo,
                "emailAge_domainRiskLevelID": emailAge_domainRiskLevelID,
                "emailAge_domainRelevantInfoID": emailAge_domainRelevantInfoID,
                "emailAge_domainRiskCountry": emailAge_domainRiskCountry,
                "smLinks_ipRiskLevel": smLinks_ipRiskLevel,
                "smLinks_ipRiskReasonId": smLinks_ipRiskReasonId,
                "smLinks_ipRiskReason": smLinks_ipRiskReason,
                "smLinks_ipReputation": smLinks_ipReputation,
                "smLinks_ipRiskLevelId": smLinks_ipRiskLevelId,
                "smLinks_ipLatitude": smLinks_ipLatitude,
                "smLinks_ipLongitude": smLinks_ipLongitude,
                "smLinks_phoneStatus": smLinks_phoneStatus,
                "smLinks_deviceIdRiskLevel": smLinks_deviceIdRiskLevel,
                "smLinks_overallDigitalIdentityScore": smLinks_overallDigitalIdentityScore,
                "smLinks_emailAge": smLinks_emailAge,
                "smLinks_fraudType": smLinks_fraudType,
                "smLinks_lastFlaggedon": smLinks_lastFlaggedon,
                "smLinks_emailExists": smLinks_emailExists,
                "smLinks_disDescription": smLinks_disDescription,
            }
        )

        del id,uniqueid,request_date,transaction_id,cpf,requestId,bureau_response_status,type,images,sections_pfData_statusCode,metadata_unique_id,metadata_tenantId,metadata_origin,metadata_templateOrigin,templateId,createdAt,fraud,bureau_response_id,statusReasons_category_0,statusReasons_code_0,statusReasons_status_0,statusReasons_resultStatus_0,statusReasons_category_1,statusReasons_code_1,statusReasons_status_1,statusReasons_resultStatus_1,statusReasons_category_2,statusReasons_code_2,statusReasons_status_2,statusReasons_resultStatus_2,statusReasons_category_3,statusReasons_code_3,statusReasons_status_3,statusReasons_resultStatus_3,statusReasons_category_4,statusReasons_code_4,statusReasons_status_4,statusReasons_resultStatus_4,attributes_cpf,event_type,status,status_reasons_category_0,status_reasons_code_0,status_reasons_status_0,status_reasons_resultStatus_0,status_reasons_description_0,status_reasons_category_1,status_reasons_code_1,status_reasons_status_1,status_reasons_resultStatus_1,status_reasons_description_1,status_reasons_category_2,status_reasons_code_2,status_reasons_status_2,status_reasons_resultStatus_2,status_reasons_description_2,status_reasons_category_3,status_reasons_code_3,status_reasons_status_3,status_reasons_resultStatus_3,status_reasons_description_3,status_reasons_category_4,status_reasons_code_4,status_reasons_status_4,status_reasons_resultStatus_4,status_reasons_description_4,status_reasons_category_5,status_reasons_code_5,status_reasons_status_5,status_reasons_resultStatus_5,status_reasons_description_5,validation_step,softon_governmentInvolvement_0,softon_involvement_0,softon_newsDate_0,softon_id_0,softon_source_0,softon_existingProcess_0,softon_suspicionType_0,softon_name_0,softon_newsDtecLink_0,softon_governmentInvolvement_1,softon_involvement_1,softon_newsDate_1,softon_id_1,softon_source_1,softon_existingProcess_1,softon_suspicionType_1,softon_name_1,softon_newsDtecLink_1,softon_governmentInvolvement_2,softon_involvement_2,softon_newsDate_2,softon_id_2,softon_source_2,softon_existingProcess_2,softon_suspicionType_2,softon_name_2,softon_newsDtecLink_2,softon_governmentInvolvement_3,softon_involvement_3,softon_newsDate_3,softon_id_3,softon_source_3,softon_existingProcess_3,softon_suspicionType_3,softon_name_3,softon_newsDtecLink_3,softon_governmentInvolvement_4,softon_involvement_4,softon_newsDate_4,softon_id_4,softon_source_4,softon_existingProcess_4,softon_suspicionType_4,softon_name_4,softon_newsDtecLink_4,pfMbsArcaMs2_statusCode,pfMbsArcaMs2_message,pfMbsArcaMs2_code,pfMbsArcaMs2_score,pfMbsArcaMs2_status,pfMbsArcaMs2_scorePLD,pfMbsArcaMs2_requestId,pfMbsArcaMs2_startAt,pfMbsArcaMs2_endAt,pfBiometriaUnico_statusCode,pfBiometriaUnico_message,pfBiometriaUnico_score,pfBiometriaUnico_errorCode,pfBiometriaUnico_startAt,pfBiometriaUnico_endAt,pfData_statusCode,pfData_taxIdStatus,pfData_birthDate,pfData_issueDate,pfData_name,ph3a_contactId,ph3a_sequencialId,ph3a_flags,ph3a_flagsList_0,ph3a_flagsList_1,ph3a_flagsList_2,ph3a_flagsList_3,ph3a_name,ph3a_gender,ph3a_document,ph3a_documentType,ph3a_ranking,ph3a_nameBrasil,ph3a_birthDate,ph3a_person_dependents,ph3a_person_nationality,ph3a_person_educationLevel,ph3a_person_motherName,ph3a_person_fatherName,ph3a_creditScore_createDate,ph3a_creditScore_status,ph3a_phones_areaCode_0,ph3a_phones_number_0,ph3a_phones_formattedNumber_0,ph3a_phones_operatorId_0,ph3a_phones_operator_0,ph3a_phones_ranking_0,ph3a_phones_createDate_0,ph3a_phones_status_0,ph3a_phones_areaCode_1,ph3a_phones_number_1,ph3a_phones_formattedNumber_1,ph3a_phones_operatorId_1,ph3a_phones_operator_1,ph3a_phones_ranking_1,ph3a_phones_createDate_1,ph3a_phones_status_1,ph3a_phones_areaCode_2,ph3a_phones_number_2,ph3a_phones_formattedNumber_2,ph3a_phones_operatorId_2,ph3a_phones_operator_2,ph3a_phones_ranking_2,ph3a_phones_createDate_2,ph3a_phones_status_2,ph3a_emails_emails_0,ph3a_emails_createDate_0,ph3a_emails_status_0,ph3a_emails_emails_1,ph3a_emails_createDate_1,ph3a_emails_status_1,ph3a_addresses_street_0,ph3a_addresses_number_0,ph3a_addresses_district_0,ph3a_addresses_zipCode_0,ph3a_addresses_city_0,ph3a_addresses_state_0,ph3a_addresses_censusSector_0,ph3a_addresses_alias_0,ph3a_addresses_shortAlias_0,ph3a_addresses_score_0,ph3a_addresses_ranking_0,ph3a_addresses_createDate_0,ph3a_addresses_status_0,ph3a_addresses_street_1,ph3a_addresses_number_1,ph3a_addresses_district_1,ph3a_addresses_zipCode_1,ph3a_addresses_city_1,ph3a_addresses_state_1,ph3a_addresses_censusSector_1,ph3a_addresses_alias_1,ph3a_addresses_shortAlias_1,ph3a_addresses_score_1,ph3a_addresses_ranking_1,ph3a_addresses_createDate_1,ph3a_addresses_status_1,ph3a_addresses_street_2,ph3a_addresses_number_2,ph3a_addresses_district_2,ph3a_addresses_zipCode_2,ph3a_addresses_city_2,ph3a_addresses_state_2,ph3a_addresses_censusSector_2,ph3a_addresses_alias_2,ph3a_addresses_shortAlias_2,ph3a_addresses_score_2,ph3a_addresses_ranking_2,ph3a_addresses_createDate_2,ph3a_addresses_status_2,emailAge_email,emailAge_ipAddress,emailAge_eName,emailAge_emailCreationDays,emailAge_domainAge,emailAge_domainCreationDays,emailAge_firstVerificationDate,emailAge_firstSeenDays,emailAge_lastVerificationDate,emailAge_status,emailAge_fraudRisk,emailAge_EAScore,emailAge_EAAdvice,emailAge_EARiskBandID,emailAge_EARiskBand,emailAge_domainName,emailAge_domainCompany,emailAge_domainCountryName,emailAge_domainCategory,emailAge_domainRiskLevel,emailAge_domainRelevantInfo,emailAge_domainRiskLevelID,emailAge_domainRelevantInfoID,emailAge_domainRiskCountry,smLinks_ipRiskLevel,smLinks_ipRiskReasonId,smLinks_ipRiskReason,smLinks_ipReputation,smLinks_ipRiskLevelId,smLinks_ipLatitude,smLinks_ipLongitude,smLinks_phoneStatus,smLinks_deviceIdRiskLevel,smLinks_overallDigitalIdentityScore,smLinks_emailAge,smLinks_fraudType,smLinks_lastFlaggedon,smLinks_emailExists,smLinks_disDescription

        info(df)

        df = df.astype(str)

        db_host = Variable.get("POSTGRES_FUNDS_HOST_PRD")
        db_name = Variable.get("POSTGRES_FUNDS_NAME_PRD")
        db_password = Variable.get("POSTGRES_FUNDS_PASSWORD_PRD")
        db_port = Variable.get("POSTGRES_FUNDS_PORT_PRD")
        db_user = Variable.get("POSTGRES_FUNDS_USER_PRD")

        postgresql_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        conn = create_engine(postgresql_url).connect()

        df.to_sql('investment_fund_bureau', con=conn, if_exists='replace', index=False)

    extract()