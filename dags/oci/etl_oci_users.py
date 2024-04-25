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
webhook_url_analytics = Variable.get("WEBHOOK_URL_ANALYTICS")
def task_failure_alert(context):
    send_slack_message('webhook_url_analytics', slack_msg="<@U04V21FNP16> , <@U05LU8M4CDP> :alert: *ERROR - Erro na inserção dos dados da collection USERS no bucket* :alert:")
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    send_slack_message(webhook_url_analytics, slack_msg="ETL Users OK")
    print(f"DAG has succeeded, run_id: {context['run_id']}")



default_args = {
    "owner": "oci",
}

with DAG(
    "etl_oci_users",
    default_args=default_args,
    description="Load users in oci",
    schedule_interval="0 10,14,18,22 * * 1-5",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["oci", "mongodb", "users"],
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
) as dag:

    @task
    def extract():        
        uri = Variable.get("MONGO_URL_PRD")

        client = MongoClient(uri)
        db = client["lionx"]
        collection = db["users"]
        content = mongo_dumps(collection.find({}))
        r = json.loads(content)

        id = []
        nick_name = []
        email = []
        unique_id = []
        created_at = []
        scope_user_level = []
        scope_view_type = []
        scope_features = []
        is_active_user = []
        must_do_first_login = []
        use_magic_link = []
        token_valid_after = []
        terms_term_application_version = []
        terms_term_application_date = []
        terms_term_application_is_deprecated = []
        terms_term_open_account_version = []
        terms_term_open_account_date = []
        terms_term_open_account_is_deprecated = []
        terms_term_retail_liquid_provider_version = []
        terms_term_retail_liquid_provider_date = []
        terms_term_retail_liquid_provider_is_deprecated = []
        terms_term_refusal_version = []
        terms_term_refusal_date = []
        terms_term_refusal_is_deprecated = []
        terms_term_non_compliance_version = []
        terms_term_non_compliance_date = []
        terms_term_non_compliance_is_deprecated = []
        terms_term_gringo_world_version = []
        terms_term_gringo_world_date = []
        terms_term_gringo_world_is_deprecated = []
        terms_term_gringo_world_general_advices_version = []
        terms_term_gringo_world_general_advices_date = []
        terms_term_gringo_world_general_advices_is_deprecated = []
        terms_term_all_agreement_gringo_dl_version = []
        terms_term_all_agreement_gringo_dl_date = []
        terms_term_all_agreement_gringo_dl_is_deprecated = []
        terms_term_and_privacy_policy_data_sharing_policy_dl_pt_version = []
        terms_term_and_privacy_policy_data_sharing_policy_dl_pt_date = []
        terms_term_and_privacy_policy_data_sharing_policy_dl_pt_is_deprecated = []
        terms_term_and_privacy_policy_data_sharing_policy_dl_us_version = []
        terms_term_and_privacy_policy_data_sharing_policy_dl_us_date = []
        terms_term_and_privacy_policy_data_sharing_policy_dl_us_is_deprecated = []
        terms_term_business_continuity_plan_dl_pt_version = []
        terms_term_business_continuity_plan_dl_pt_date = []
        terms_term_business_continuity_plan_dl_pt_is_deprecated = []
        terms_term_business_continuity_plan_dl_us_version = []
        terms_term_business_continuity_plan_dl_us_date = []
        terms_term_business_continuity_plan_dl_us_is_deprecated = []
        terms_term_customer_relationship_summary_dl_pt_version = []
        terms_term_customer_relationship_summary_dl_pt_date = []
        terms_term_customer_relationship_summary_dl_pt_is_deprecated = []
        terms_term_customer_relationship_summary_dl_us_version = []
        terms_term_customer_relationship_summary_dl_us_date = []
        terms_term_customer_relationship_summary_dl_us_is_deprecated = []
        terms_term_open_account_dl_pt_version = []
        terms_term_open_account_dl_pt_date = []
        terms_term_open_account_dl_pt_is_deprecated = []
        terms_term_open_account_dl_us_version = []
        terms_term_open_account_dl_us_date = []
        terms_term_open_account_dl_us_is_deprecated = []
        terms_term_ouroinvest_version = []
        terms_term_ouroinvest_date = []
        terms_term_ouroinvest_is_deprecated = []
        suitability_score = []
        suitability_profile = []
        suitability_submission_date = []
        suitability_version = []
        identifier_document_cpf = []
        identifier_document_document_data_type = []
        identifier_document_document_data_number = []
        identifier_document_document_issuer = []
        identifier_document_document_state = []
        phone = []
        bureau_status = []
        is_bureau_data_validated = []
        marital_status = []
        marital_spouse_nationality = []
        marital_spouse_cpf = []
        marital_spouse_name = []
        address_country = []
        address_street_name = []
        address_city = []
        address_number = []
        address_zip_code = []
        address_neighborhood = []
        address_state = []
        address_complement = []
        assets_patrimony = []
        assets_income = []
        assets_date = []
        birth_date = []
        birth_place_city = []
        birth_place_country = []
        birth_place_state = []
        father_name = []
        gender = []
        mother_name = []
        name = []
        nationality = []
        occupation_activity = []
        occupation_company_cnpj = []
        occupation_company_name = []
        can_be_managed_by_third_party_operator = []
        is_active_client = []
        is_managed_by_third_party_operator = []
        is_registration_update = []
        last_modified_date = []
        portfolios_br_bovespa = []
        portfolios_br_bmf = []
        portfolios_br_created_at = []
        portfolios_us_dw_id = []
        portfolios_us_created_at = []
        portfolios_us_dw_display_account = []
        sinacor = []
        sincad = []
        solutiontech = []
        is_third_party_operator = []
        third_party_operator_details = []
        third_party_operator_email = []
        electronic_signature = []
        electronic_signature_wrong_attempts = []
        is_blocked_electronic_signature = []
        bank_accounts_bank_0 = []
        bank_accounts_account_type_0 = []
        bank_accounts_agency_0 = []
        bank_accounts_account_name_0 = []
        bank_accounts_account_number_0 = []
        bank_accounts_id_0 = []
        bank_accounts_status_0 = []
        bank_accounts_bank_1 = []
        bank_accounts_account_type_1 = []
        bank_accounts_agency_1 = []
        bank_accounts_account_name_1 = []
        bank_accounts_account_number_1 = []
        bank_accounts_id_1 = []
        bank_accounts_status_1 = []
        email_validated = []
        origin = []
        sinacor_data_cd_con_dep = []
        sinacor_data_in_irsdiv = []
        sinacor_data_in_pess_vinc = []
        sinacor_data_tp_cliente = []
        sinacor_data_tp_pessoa = []
        sinacor_data_tp_investidor = []
        sinacor_data_in_situac_cliger = []
        sinacor_data_cd_cosif = []
        sinacor_data_cd_cosif_ci = []
        sinacor_data_in_rec_divi = []
        sinacor_data_in_ende = []
        sinacor_data_cod_finl_end1 = []
        sinacor_data_cd_origem = []
        sinacor_data_in_cart_prop = []
        sinacor_data_in_emite_nota = []
        sinacor_data_in_situac = []
        sinacor_data_pc_corcor_prin = []
        sinacor_data_tp_cliente_bol = []
        sinacor_data_tp_investidor_bol = []
        sinacor_data_ind_pcta = []
        sinacor_data_in_emite_nota_cs = []
        sinacor_data_ind_end_vinc_con = []
        sinacor_data_ind_env_email_bvmf = []
        sinacor_data_tp_cliente_bmf = []
        sinacor_data_ind_opcr_agnt_td = []
        sinacor_data_cod_tipo_colt = []
        sinacor_data_cod_cep_estr1 = []
        sinacor_data_uf_estr1 = []
        sinacor_data_num_class_risc_cmtt = []
        sinacor_data_desc_risc_cmtt = []
        sinacor_data_pc_corcor_prin_cs = []
        sinacor_data_in_tipo_corret_exec_cs = []
        sinacor_data_num_aut_tra_orde_prcd = []
        sinacor_data_user_account_block = []
        us_person = []
        bureau_validations_cpf = []
        bureau_validations_score = []
        external_exchange_requirements_us_is_politically_exposed = []
        external_exchange_requirements_us_politically_exposed_names = []
        external_exchange_requirements_us_is_exchange_member = []
        external_exchange_requirements_us_company_ticker_that_user_is_director_of = []
        external_exchange_requirements_us_is_company_director = []
        external_exchange_requirements_us_is_company_director_of = []
        external_exchange_requirements_us_external_fiscal_tax_confirmation = []
        external_exchange_requirements_us_user_employ_company_name = []
        external_exchange_requirements_us_user_employ_position = []
        external_exchange_requirements_us_user_employ_status = []
        external_exchange_requirements_us_time_experience = []
        external_exchange_requirements_us_w8_confirmation = []
        dw = []
        ouro_invest_onboarding_code = []
        ouro_invest_status = []
        email_updated_at = []
        origin_id = []
        third_party_sync_picpay = []
        is_correlated_to_politically_exposed_person = []
        is_politically_exposed_person = []
        pld_rating = []
        pld_score = []
        last_registration_data_update = []
        sinacor_account_block_status = []
        external_exchange_requirements_us_user_employ_type = []
        current_pld_risk_rating_defined_in = []
        portfolios_us_dw_account = []
        currentStep = []
        steps_import = []
        steps_creation = []
        steps_birth = []
        steps_matrimony = []
        steps_usPerson = []
        steps_document = []
        steps_address = []
        steps_occupation = []
        steps_patrimony = []
        steps_review = []
        expiration_dates_suitability = []


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
            nick_name.append(str(verifica_valor(r[i], "nick_name")).replace("'2", "2"))
            email.append(verifica_valor(r[i], "email"))
            unique_id.append(verifica_valor(r[i], "unique_id"))
            created_at.append(verifica_valor(r[i], "created_at", "$date"))
            scope_user_level.append(verifica_valor(r[i], "scope", "user_level"))
            scope_view_type.append(verifica_valor(r[i], "scope", "view_type"))
            scope_features.append(verifica_valor(r[i], "scope", "features", 0))
            is_active_user.append(verifica_valor(r[i], "is_active_user"))
            must_do_first_login.append(verifica_valor(r[i], "must_do_first_login"))
            use_magic_link.append(verifica_valor(r[i], "use_magic_link"))
            token_valid_after.append(verifica_valor(r[i], "token_valid_after", "date", "$date"))
            terms_term_application_version.append(verifica_valor(r[i], "terms", "term_application", "version"))
            terms_term_application_date.append(verifica_valor(r[i], "terms", "term_application", "date", "$date"))
            terms_term_application_is_deprecated.append(verifica_valor(r[i], "terms", "term_application", "is_deprecated"))
            terms_term_open_account_version.append(verifica_valor(r[i], "terms", "term_open_account", "version"))
            terms_term_open_account_date.append(verifica_valor(r[i], "terms", "term_open_account", "date", "$date"))
            terms_term_open_account_is_deprecated.append(verifica_valor(r[i], "terms", "term_open_account", "is_deprecated"))
            terms_term_retail_liquid_provider_version.append(verifica_valor(r[i], "terms", "term_retail_liquid_provider", "version"))
            terms_term_retail_liquid_provider_date.append(verifica_valor(r[i], "terms", "term_retail_liquid_provider", "date", "$date"))
            terms_term_retail_liquid_provider_is_deprecated.append(verifica_valor(r[i], "terms", "term_retail_liquid_provider", "is_deprecated"))
            terms_term_refusal_version.append(verifica_valor(r[i], "terms", "term_refusal", "version"))
            terms_term_refusal_date.append(verifica_valor(r[i], "terms", "term_refusal", "date", "$date"))
            terms_term_refusal_is_deprecated.append(verifica_valor(r[i], "terms", "term_refusal", "is_deprecated"))
            terms_term_non_compliance_version.append(verifica_valor(r[i], "terms", "term_non_compliance", "version"))
            terms_term_non_compliance_date.append(verifica_valor(r[i], "terms", "term_non_compliance", "date", "$date"))
            terms_term_non_compliance_is_deprecated.append(verifica_valor(r[i], "terms", "term_non_compliance", "is_deprecated"))
            terms_term_gringo_world_version.append(verifica_valor(r[i], "terms", "term_gringo_world", "version"))
            terms_term_gringo_world_date.append(verifica_valor(r[i], "terms", "term_gringo_world", "date", "$date"))
            terms_term_gringo_world_is_deprecated.append(verifica_valor(r[i], "terms", "term_gringo_world", "is_deprecated"))
            terms_term_gringo_world_general_advices_version.append(verifica_valor(r[i], "terms", "term_gringo_world_general_advices", "version"))
            terms_term_gringo_world_general_advices_date.append(verifica_valor(r[i], "terms", "term_gringo_world_general_advices", "date", "$date"))
            terms_term_gringo_world_general_advices_is_deprecated.append(verifica_valor(r[i], "terms", "term_gringo_world_general_advices", "is_deprecated"))
            terms_term_all_agreement_gringo_dl_version.append(verifica_valor(r[i], "terms", "term_all_agreement_gringo_dl", "version"))
            terms_term_all_agreement_gringo_dl_date.append(verifica_valor(r[i], "terms", "term_all_agreement_gringo_dl", "date", "$date"))
            terms_term_all_agreement_gringo_dl_is_deprecated.append(verifica_valor(r[i], "terms", "term_all_agreement_gringo_dl", "is_deprecated"))
            terms_term_and_privacy_policy_data_sharing_policy_dl_pt_version.append(verifica_valor(r[i], "terms", "term_and_privacy_policy_data_sharing_policy_dl_pt", "version"))
            terms_term_and_privacy_policy_data_sharing_policy_dl_pt_date.append(verifica_valor(r[i], "terms", "term_and_privacy_policy_data_sharing_policy_dl_pt", "date", "$date"))
            terms_term_and_privacy_policy_data_sharing_policy_dl_pt_is_deprecated.append(verifica_valor(r[i], "terms", "term_and_privacy_policy_data_sharing_policy_dl_pt", "is_deprecated"))
            terms_term_and_privacy_policy_data_sharing_policy_dl_us_version.append(verifica_valor(r[i], "terms", "term_and_privacy_policy_data_sharing_policy_dl_us", "version"))
            terms_term_and_privacy_policy_data_sharing_policy_dl_us_date.append(verifica_valor(r[i], "terms", "term_and_privacy_policy_data_sharing_policy_dl_us", "date", "$date"))
            terms_term_and_privacy_policy_data_sharing_policy_dl_us_is_deprecated.append(verifica_valor(r[i], "terms", "term_and_privacy_policy_data_sharing_policy_dl_us", "is_deprecated"))
            terms_term_business_continuity_plan_dl_pt_version.append(verifica_valor(r[i], "terms", "term_business_continuity_plan_dl_pt", "version"))
            terms_term_business_continuity_plan_dl_pt_date.append(verifica_valor(r[i], "terms", "term_business_continuity_plan_dl_pt", "date", "$date"))
            terms_term_business_continuity_plan_dl_pt_is_deprecated.append(verifica_valor(r[i], "terms", "term_business_continuity_plan_dl_pt", "is_deprecated"))
            terms_term_business_continuity_plan_dl_us_version.append(verifica_valor(r[i], "terms", "term_business_continuity_plan_dl_us", "version"))
            terms_term_business_continuity_plan_dl_us_date.append(verifica_valor(r[i], "terms", "term_business_continuity_plan_dl_us", "date", "$date"))
            terms_term_business_continuity_plan_dl_us_is_deprecated.append(verifica_valor(r[i], "terms", "term_business_continuity_plan_dl_us", "is_deprecated"))
            terms_term_customer_relationship_summary_dl_pt_version.append(verifica_valor(r[i], "terms", "term_customer_relationship_summary_dl_pt", "version"))
            terms_term_customer_relationship_summary_dl_pt_date.append(verifica_valor(r[i], "terms", "term_customer_relationship_summary_dl_pt", "date", "$date"))
            terms_term_customer_relationship_summary_dl_pt_is_deprecated.append(verifica_valor(r[i], "terms", "term_customer_relationship_summary_dl_pt", "is_deprecated"))
            terms_term_customer_relationship_summary_dl_us_version.append(verifica_valor(r[i], "terms", "term_customer_relationship_summary_dl_us", "version"))
            terms_term_customer_relationship_summary_dl_us_date.append(verifica_valor(r[i], "terms", "term_customer_relationship_summary_dl_us", "date", "$date"))
            terms_term_customer_relationship_summary_dl_us_is_deprecated.append(verifica_valor(r[i], "terms", "term_customer_relationship_summary_dl_us", "is_deprecated"))
            terms_term_open_account_dl_pt_version.append(verifica_valor(r[i], "terms", "term_open_account_dl_pt", "version"))
            terms_term_open_account_dl_pt_date.append(verifica_valor(r[i], "terms", "term_open_account_dl_pt", "date", "$date"))
            terms_term_open_account_dl_pt_is_deprecated.append(verifica_valor(r[i], "terms", "term_open_account_dl_pt", "is_deprecated"))
            terms_term_open_account_dl_us_version.append(verifica_valor(r[i], "terms", "term_open_account_dl_us", "version"))
            terms_term_open_account_dl_us_date.append(verifica_valor(r[i], "terms", "term_open_account_dl_us", "date", "$date"))
            terms_term_open_account_dl_us_is_deprecated.append(verifica_valor(r[i], "terms", "term_open_account_dl_us", "is_deprecated"))
            terms_term_ouroinvest_version.append(verifica_valor(r[i], "terms", "term_ouroinvest", "version"))
            terms_term_ouroinvest_date.append(verifica_valor(r[i], "terms", "term_ouroinvest", "date", "$date"))
            terms_term_ouroinvest_is_deprecated.append(verifica_valor(r[i], "terms", "term_ouroinvest", "is_deprecated"))
            suitability_score.append(verifica_valor(r[i], "suitability", "score"))
            suitability_profile.append(verifica_valor(r[i], "suitability", "profile"))
            suitability_submission_date.append(verifica_valor(r[i], "suitability", "submission_date", "$date"))
            suitability_version.append(verifica_valor(r[i], "suitability", "suitability_version"))
            identifier_document_cpf.append(verifica_valor(r[i], "identifier_document", "cpf"))
            identifier_document_document_data_type.append(verifica_valor(r[i], "identifier_document", "document_data", "type"))
            identifier_document_document_data_number.append(verifica_valor(r[i], "identifier_document", "document_data", "number"))
            identifier_document_document_issuer.append(verifica_valor(r[i], "identifier_document", "document_data", "issuer"))
            identifier_document_document_state.append(verifica_valor(r[i], "identifier_document", "document_data", "state"))
            phone.append(verifica_valor(r[i], "phone"))
            bureau_status.append(verifica_valor(r[i], "bureau_status"))
            is_bureau_data_validated.append(verifica_valor(r[i], "is_bureau_data_validated"))
            marital_status.append(verifica_valor(r[i], "marital", "status"))
            marital_spouse_nationality.append(verifica_valor(r[i], "marital", "spouse", "nationality"))
            marital_spouse_cpf.append(verifica_valor(r[i], "marital", "spouse", "cpf"))
            marital_spouse_name.append(verifica_valor(r[i], "marital", "spouse", "name"))
            address_country.append(verifica_valor(r[i], "address", "country"))
            address_street_name.append(verifica_valor(r[i], "address", "street_name"))
            address_city.append(verifica_valor(r[i], "address", "city"))
            address_number.append(verifica_valor(r[i], "address", "number"))
            address_zip_code.append(verifica_valor(r[i], "address", "zip_code"))
            address_neighborhood.append(verifica_valor(r[i], "address", "neighborhood"))
            address_state.append(verifica_valor(r[i], "address", "state"))
            address_complement.append(verifica_valor(r[i], "address", "complement"))
            assets_patrimony.append(verifica_valor(r[i], "assets", "patrimony"))
            assets_income.append(verifica_valor(r[i], "assets", "income"))
            assets_date.append(verifica_valor(r[i], "assets", "date", "$date"))
            birth_date.append(verifica_valor(r[i], "birth_date", "$date"))
            birth_place_city.append(verifica_valor(r[i], "birth_place_city"))
            birth_place_country.append(verifica_valor(r[i], "birth_place_country"))
            birth_place_state.append(verifica_valor(r[i], "birth_place_state"))
            father_name.append(verifica_valor(r[i], "father_name"))
            gender.append(verifica_valor(r[i], "gender"))
            mother_name.append(verifica_valor(r[i], "mother_name"))
            name.append(verifica_valor(r[i], "name"))
            nationality.append(verifica_valor(r[i], "nationality"))
            occupation_activity.append(verifica_valor(r[i], "occupation", "activity"))
            occupation_company_cnpj.append(verifica_valor(r[i], "occupation", "company", "cnpj"))
            occupation_company_name.append(verifica_valor(r[i], "occupation", "company", "name"))
            can_be_managed_by_third_party_operator.append(verifica_valor(r[i], "can_be_managed_by_third_party_operator"))
            is_active_client.append(verifica_valor(r[i], "is_active_client"))
            is_managed_by_third_party_operator.append(verifica_valor(r[i], "is_managed_by_third_party_operator"))
            is_registration_update.append(verifica_valor(r[i], "is_registration_update"))
            last_modified_date.append(verifica_valor(r[i], "last_modified_date", "concluded_at", "$date"))
            portfolios_br_bovespa.append(verifica_valor(r[i], "portfolios", "default", "br", "bovespa_account"))
            portfolios_br_bmf.append(verifica_valor(r[i], "portfolios", "default", "br", "bmf_account"))
            portfolios_br_created_at.append(verifica_valor(r[i], "portfolios", "default", "br", "created_at", "$date"))
            portfolios_us_dw_id.append(verifica_valor(r[i], "portfolios", "default", "us", "dw_id"))
            portfolios_us_created_at.append(verifica_valor(r[i], "portfolios", "default", "us", "created_at", "$date"))
            portfolios_us_dw_account.append(verifica_valor(r[i], "portfolios", "default", "us", "dw_account"))
            portfolios_us_dw_display_account.append(verifica_valor(r[i], "portfolios", "default", "us", "dw_display_account"))
            sinacor.append(verifica_valor(r[i], "sinacor"))
            sincad.append(verifica_valor(r[i], "sincad"))
            solutiontech.append(verifica_valor(r[i], "solutiontech"))
            is_third_party_operator.append(verifica_valor(r[i], "third_party_operator", "is_third_party_operator"))
            third_party_operator_details.append(verifica_valor(r[i], "third_party_operator", "details"))
            third_party_operator_email.append(verifica_valor(r[i], "third_party_operator", "third_party_operator_email"))
            electronic_signature.append(verifica_valor(r[i], "electronic_signature"))
            electronic_signature_wrong_attempts.append(verifica_valor(r[i], "electronic_signature_wrong_attempts"))
            is_blocked_electronic_signature.append(verifica_valor(r[i], "is_blocked_electronic_signature"))
            bank_accounts_bank_0.append(verifica_valor(r[i], "bank_accounts", 0, "bank"))
            bank_accounts_account_type_0.append(verifica_valor(r[i], "bank_accounts", 0, "account_type"))
            bank_accounts_agency_0.append(verifica_valor(r[i], "bank_accounts", 0, "agency"))
            bank_accounts_account_name_0.append(verifica_valor(r[i], "bank_accounts", 0, "account_name"))
            bank_accounts_account_number_0.append(verifica_valor(r[i], "bank_accounts", 0, "account_number"))
            bank_accounts_id_0.append(verifica_valor(r[i], "bank_accounts", 0, "id"))
            bank_accounts_status_0.append(verifica_valor(r[i], "bank_accounts", 0, "status"))
            bank_accounts_bank_1.append(verifica_valor(r[i], "bank_accounts", 1, "bank"))
            bank_accounts_account_type_1.append(verifica_valor(r[i], "bank_accounts", 1, "account_type"))
            bank_accounts_agency_1.append(verifica_valor(r[i], "bank_accounts", 1, "agency"))
            bank_accounts_account_name_1.append(verifica_valor(r[i], "bank_accounts", 1, "account_name"))
            bank_accounts_account_number_1.append(verifica_valor(r[i], "bank_accounts", 1, "account_number"))
            bank_accounts_id_1.append(verifica_valor(r[i], "bank_accounts", 1, "id"))
            bank_accounts_status_1.append(verifica_valor(r[i], "bank_accounts", 1, "status"))
            email_validated.append(verifica_valor(r[i], "email_validated"))
            origin.append(verifica_valor(r[i], "origin"))
            sinacor_data_cd_con_dep.append(verifica_valor(r[i], "sinacor_data", "cd_con_dep"))
            sinacor_data_in_irsdiv.append(verifica_valor(r[i], "sinacor_data", "in_irsdiv"))
            sinacor_data_in_pess_vinc.append(verifica_valor(r[i], "sinacor_data", "in_pess_vinc"))
            sinacor_data_tp_cliente.append(verifica_valor(r[i], "sinacor_data", "tp_cliente"))
            sinacor_data_tp_pessoa.append(verifica_valor(r[i], "sinacor_data", "tp_pessoa"))
            sinacor_data_tp_investidor.append(verifica_valor(r[i], "sinacor_data", "tp_investidor"))
            sinacor_data_in_situac_cliger.append(verifica_valor(r[i], "sinacor_data", "in_situac_cliger"))
            sinacor_data_cd_cosif.append(verifica_valor(r[i], "sinacor_data", "cd_cosif"))
            sinacor_data_cd_cosif_ci.append(verifica_valor(r[i], "sinacor_data", "cd_cosif_ci"))
            sinacor_data_in_rec_divi.append(verifica_valor(r[i], "sinacor_data", "in_rec_divi"))
            sinacor_data_in_ende.append(verifica_valor(r[i], "sinacor_data", "in_ende"))
            sinacor_data_cod_finl_end1.append(verifica_valor(r[i], "sinacor_data", "cod_finl_end1"))
            sinacor_data_cd_origem.append(verifica_valor(r[i], "sinacor_data", "cd_origem"))
            sinacor_data_in_cart_prop.append(verifica_valor(r[i], "sinacor_data", "in_cart_prop"))
            sinacor_data_in_emite_nota.append(verifica_valor(r[i], "sinacor_data", "in_emite_nota"))
            sinacor_data_in_situac.append(verifica_valor(r[i], "sinacor_data", "in_situac"))
            sinacor_data_pc_corcor_prin.append(verifica_valor(r[i], "sinacor_data", "pc_corcor_prin"))
            sinacor_data_tp_cliente_bol.append(verifica_valor(r[i], "sinacor_data", "tp_cliente_bol"))
            sinacor_data_tp_investidor_bol.append(verifica_valor(r[i], "sinacor_data", "tp_investidor_bol"))
            sinacor_data_ind_pcta.append(verifica_valor(r[i], "sinacor_data", "ind_pcta"))
            sinacor_data_in_emite_nota_cs.append(verifica_valor(r[i], "sinacor_data", "in_emite_nota_cs"))
            sinacor_data_ind_end_vinc_con.append(verifica_valor(r[i], "sinacor_data", "ind_end_vinc_con"))
            sinacor_data_ind_env_email_bvmf.append(verifica_valor(r[i], "sinacor_data", "ind_env_email_bvmf"))
            sinacor_data_tp_cliente_bmf.append(verifica_valor(r[i], "sinacor_data", "tp_cliente_bmf"))
            sinacor_data_ind_opcr_agnt_td.append(verifica_valor(r[i], "sinacor_data", "ind_opcr_agnt_td"))
            sinacor_data_cod_tipo_colt.append(verifica_valor(r[i], "sinacor_data", "cod_tipo_colt"))
            sinacor_data_cod_cep_estr1.append(verifica_valor(r[i], "sinacor_data", "cod_cep_estr1"))
            sinacor_data_uf_estr1.append(verifica_valor(r[i], "sinacor_data", "uf_estr1"))
            sinacor_data_num_class_risc_cmtt.append(verifica_valor(r[i], "sinacor_data", "num_class_risc_cmtt"))
            sinacor_data_desc_risc_cmtt.append(verifica_valor(r[i], "sinacor_data", "desc_risc_cmtt"))
            sinacor_data_pc_corcor_prin_cs.append(verifica_valor(r[i], "sinacor_data", "pc_corcor_prin_cs"))
            sinacor_data_in_tipo_corret_exec_cs.append(verifica_valor(r[i], "sinacor_data", "in_tipo_corret_exec_cs"))
            sinacor_data_num_aut_tra_orde_prcd.append(verifica_valor(r[i], "sinacor_data", "num_aut_tra_orde_prcd"))
            sinacor_data_user_account_block.append(verifica_valor(r[i], "sinacor_data", "user_account_block"))
            us_person.append(verifica_valor(r[i], "us_person"))
            bureau_validations_cpf.append(verifica_valor(r[i], "bureau_validations", "cpf"))
            bureau_validations_score.append(verifica_valor(r[i], "bureau_validations", "score"))
            external_exchange_requirements_us_is_politically_exposed.append(verifica_valor(r[i], "external_exchange_requirements", "us", "is_politically_exposed"))
            external_exchange_requirements_us_politically_exposed_names.append(verifica_valor(r[i], "external_exchange_requirements", "us", "politically_exposed_names"))
            external_exchange_requirements_us_is_exchange_member.append(verifica_valor(r[i], "external_exchange_requirements", "us", "is_exchange_member"))
            external_exchange_requirements_us_company_ticker_that_user_is_director_of.append(verifica_valor(r[i], "external_exchange_requirements", "us", "company_ticker_that_user_is_director_of"))
            external_exchange_requirements_us_is_company_director.append(verifica_valor(r[i], "external_exchange_requirements", "us", "is_company_director"))
            external_exchange_requirements_us_is_company_director_of.append(verifica_valor(r[i], "external_exchange_requirements", "us", "is_company_director_of"))
            external_exchange_requirements_us_external_fiscal_tax_confirmation.append(verifica_valor(r[i], "external_exchange_requirements", "us", "external_fiscal_tax_confirmation"))
            external_exchange_requirements_us_user_employ_company_name.append(verifica_valor(r[i], "external_exchange_requirements", "us", "user_employ_company_name"))
            external_exchange_requirements_us_user_employ_position.append(verifica_valor(r[i], "external_exchange_requirements", "us", "user_employ_position"))
            external_exchange_requirements_us_user_employ_status.append(verifica_valor(r[i], "external_exchange_requirements", "us", "user_employ_status"))
            external_exchange_requirements_us_user_employ_type.append(verifica_valor(r[i], "external_exchange_requirements", "us", "user_employ_type"))
            external_exchange_requirements_us_time_experience.append(verifica_valor(r[i], "external_exchange_requirements", "us", "time_experience"))
            external_exchange_requirements_us_w8_confirmation.append(verifica_valor(r[i], "external_exchange_requirements", "us", "w8_confirmation"))
            dw.append(verifica_valor(r[i], "dw"))
            ouro_invest_onboarding_code.append(verifica_valor(r[i], "ouro_invest", "onboarding_code"))
            ouro_invest_status.append(verifica_valor(r[i], "ouro_invest", "status"))
            email_updated_at.append(verifica_valor(r[i], "email_updated_at", "$date"))
            origin_id.append(verifica_valor(r[i], "origin_id"))
            third_party_sync_picpay.append(verifica_valor(r[i], "third_party_sync", "picpay"))
            is_correlated_to_politically_exposed_person.append(verifica_valor(r[i], "is_correlated_to_politically_exposed_person"))
            is_politically_exposed_person.append(verifica_valor(r[i], "is_politically_exposed_person"))
            pld_rating.append(verifica_valor(r[i], "pld", "rating"))
            pld_score.append(verifica_valor(r[i], "pld", "score"))
            last_registration_data_update.append(verifica_valor(r[i],"record_date_control","registry_updates","last_registration_data_update","$date",))
            current_pld_risk_rating_defined_in.append(verifica_valor(r[i], "record_date_control", "current_pld_risk_rating_defined_in", "$date"))
            sinacor_account_block_status.append(verifica_valor(r[i], "sinacor_account_block_status"))
            currentStep.append(verifica_valor(r[i], "currentStep"))
            steps_import.append(verifica_valor(r[i], "steps", "import"))
            steps_creation.append(verifica_valor(r[i], "steps", "creation"))
            steps_birth.append(verifica_valor(r[i], "steps", "birth"))
            steps_matrimony.append(verifica_valor(r[i], "steps", "matrimony"))
            steps_usPerson.append(verifica_valor(r[i], "steps", "usPerson"))
            steps_document.append(verifica_valor(r[i], "steps", "document"))
            steps_address.append(verifica_valor(r[i], "steps", "address"))
            steps_occupation.append(verifica_valor(r[i], "steps", "occupation"))
            steps_patrimony.append(verifica_valor(r[i], "steps", "patrimony"))
            steps_review.append(verifica_valor(r[i], "steps", "review"))
            expiration_dates_suitability.append(verifica_valor(r[i], "expiration_dates", "suitability","$date","$numberLong"))

        df = pd.DataFrame(
            {
                "id": id,
                "nick_name": nick_name,
                "email": email,
                "unique_id": unique_id,
                "created_at": created_at,
                "scope_user_level": scope_user_level,
                "scope_view_type": scope_view_type,
                "scope_features": scope_features,
                "is_active_user": is_active_user,
                "must_do_first_login": must_do_first_login,
                "use_magic_link": use_magic_link,
                "token_valid_after": token_valid_after,
                "terms_term_application_version": terms_term_application_version,
                "terms_term_application_date": terms_term_application_date,
                "terms_term_application_is_deprecated": terms_term_application_is_deprecated,
                "terms_term_open_account_version": terms_term_open_account_version,
                "terms_term_open_account_date": terms_term_open_account_date,
                "terms_term_open_account_is_deprecated": terms_term_open_account_is_deprecated,
                "terms_term_retail_liquid_provider_version": terms_term_retail_liquid_provider_version,
                "terms_term_retail_liquid_provider_date": terms_term_retail_liquid_provider_date,
                "terms_term_retail_liquid_provider_is_deprecated": terms_term_retail_liquid_provider_is_deprecated,
                "terms_term_refusal_version": terms_term_refusal_version,
                "terms_term_refusal_date": terms_term_refusal_date,
                "terms_term_refusal_is_deprecated": terms_term_refusal_is_deprecated,
                "terms_term_non_compliance_version": terms_term_non_compliance_version,
                "terms_term_non_compliance_date": terms_term_non_compliance_date,
                "terms_term_non_compliance_is_deprecated": terms_term_non_compliance_is_deprecated,
                "terms_term_gringo_world_version": terms_term_gringo_world_version,
                "terms_term_gringo_world_date": terms_term_gringo_world_date,
                "terms_term_gringo_world_is_deprecated": terms_term_gringo_world_is_deprecated,
                "terms_term_gringo_world_general_advices_version": terms_term_gringo_world_general_advices_version,
                "terms_term_gringo_world_general_advices_date": terms_term_gringo_world_general_advices_date,
                "terms_term_gringo_world_general_advices_is_deprecated": terms_term_gringo_world_general_advices_is_deprecated,
                "terms_term_all_agreement_gringo_dl_version": terms_term_all_agreement_gringo_dl_version,
                "terms_term_all_agreement_gringo_dl_date": terms_term_all_agreement_gringo_dl_date,
                "terms_term_all_agreement_gringo_dl_is_deprecated": terms_term_all_agreement_gringo_dl_is_deprecated,
                "terms_term_and_privacy_policy_data_sharing_policy_dl_pt_version": terms_term_and_privacy_policy_data_sharing_policy_dl_pt_version,
                "terms_term_and_privacy_policy_data_sharing_policy_dl_pt_date": terms_term_and_privacy_policy_data_sharing_policy_dl_pt_date,
                "terms_term_and_privacy_policy_data_sharing_policy_dl_pt_is_deprecated": terms_term_and_privacy_policy_data_sharing_policy_dl_pt_is_deprecated,
                "terms_term_and_privacy_policy_data_sharing_policy_dl_us_version": terms_term_and_privacy_policy_data_sharing_policy_dl_us_version,
                "terms_term_and_privacy_policy_data_sharing_policy_dl_us_date": terms_term_and_privacy_policy_data_sharing_policy_dl_us_date,
                "terms_term_and_privacy_policy_data_sharing_policy_dl_us_is_deprecated": terms_term_and_privacy_policy_data_sharing_policy_dl_us_is_deprecated,
                "terms_term_business_continuity_plan_dl_pt_version": terms_term_business_continuity_plan_dl_pt_version,
                "terms_term_business_continuity_plan_dl_pt_date": terms_term_business_continuity_plan_dl_pt_date,
                "terms_term_business_continuity_plan_dl_pt_is_deprecated": terms_term_business_continuity_plan_dl_pt_is_deprecated,
                "terms_term_business_continuity_plan_dl_us_version": terms_term_business_continuity_plan_dl_us_version,
                "terms_term_business_continuity_plan_dl_us_date": terms_term_business_continuity_plan_dl_us_date,
                "terms_term_business_continuity_plan_dl_us_is_deprecated": terms_term_business_continuity_plan_dl_us_is_deprecated,
                "terms_term_customer_relationship_summary_dl_pt_version": terms_term_customer_relationship_summary_dl_pt_version,
                "terms_term_customer_relationship_summary_dl_pt_date": terms_term_customer_relationship_summary_dl_pt_date,
                "terms_term_customer_relationship_summary_dl_pt_is_deprecated": terms_term_customer_relationship_summary_dl_pt_is_deprecated,
                "terms_term_customer_relationship_summary_dl_us_version": terms_term_customer_relationship_summary_dl_us_version,
                "terms_term_customer_relationship_summary_dl_us_date": terms_term_customer_relationship_summary_dl_us_date,
                "terms_term_customer_relationship_summary_dl_us_is_deprecated": terms_term_customer_relationship_summary_dl_us_is_deprecated,
                "terms_term_open_account_dl_pt_version": terms_term_open_account_dl_pt_version,
                "terms_term_open_account_dl_pt_date": terms_term_open_account_dl_pt_date,
                "terms_term_open_account_dl_pt_is_deprecated": terms_term_open_account_dl_pt_is_deprecated,
                "terms_term_open_account_dl_us_version": terms_term_open_account_dl_us_version,
                "terms_term_open_account_dl_us_date": terms_term_open_account_dl_us_date,
                "terms_term_open_account_dl_us_is_deprecated": terms_term_open_account_dl_us_is_deprecated,
                "terms_term_ouroinvest_version": terms_term_ouroinvest_version,
                "terms_term_ouroinvest_date": terms_term_ouroinvest_date,
                "terms_term_ouroinvest_is_deprecated": terms_term_ouroinvest_is_deprecated,
                "suitability_score": suitability_score,
                "suitability_profile": suitability_profile,
                "suitability_submission_date": suitability_submission_date,
                "suitability_version": suitability_version,
                "identifier_document_cpf": identifier_document_cpf,
                "identifier_document_document_data_type": identifier_document_document_data_type,
                "identifier_document_document_data_number": identifier_document_document_data_number,
                "identifier_document_document_issuer": identifier_document_document_issuer,
                "identifier_document_document_state": identifier_document_document_state,
                "phone": phone,
                "bureau_status": bureau_status,
                "is_bureau_data_validated": is_bureau_data_validated,
                "marital_status": marital_status,
                "marital_spouse_nationality": marital_spouse_nationality,
                "marital_spouse_cpf": marital_spouse_cpf,
                "marital_spouse_name": marital_spouse_name,
                "address_country": address_country,
                "address_street_name": address_street_name,
                "address_city": address_city,
                "address_number": address_number,
                "address_zip_code": address_zip_code,
                "address_neighborhood": address_neighborhood,
                "address_state": address_state,
                "address_complement": address_complement,
                "assets_patrimony": assets_patrimony,
                "assets_income": assets_income,
                "assets_date": assets_date,
                "birth_date": birth_date,
                "birth_place_city": birth_place_city,
                "birth_place_country": birth_place_country,
                "birth_place_state": birth_place_state,
                "father_name": father_name,
                "gender": gender,
                "mother_name": mother_name,
                "name": name,
                "nationality": nationality,
                "occupation_activity": occupation_activity,
                "occupation_company_cnpj": occupation_company_cnpj,
                "occupation_company_name": occupation_company_name,
                "can_be_managed_by_third_party_operator": can_be_managed_by_third_party_operator,
                "is_active_client": is_active_client,
                "is_managed_by_third_party_operator": is_managed_by_third_party_operator,
                "is_registration_update": is_registration_update,
                "last_modified_date": last_modified_date,
                "portfolios_br_bovespa": portfolios_br_bovespa,
                "portfolios_br_bmf": portfolios_br_bmf,
                "portfolios_br_created_at": portfolios_br_created_at,
                "portfolios_us_dw_id": portfolios_us_dw_id,
                "portfolios_us_created_at": portfolios_us_created_at,
                "portfolios_us_dw_display_account": portfolios_us_dw_display_account,
                "sinacor": sinacor,
                "sincad": sincad,
                "solutiontech": solutiontech,
                "is_third_party_operator": is_third_party_operator,
                "third_party_operator_details": third_party_operator_details,
                "third_party_operator_email": third_party_operator_email,
                "electronic_signature": electronic_signature,
                "electronic_signature_wrong_attempts": electronic_signature_wrong_attempts,
                "is_blocked_electronic_signature": is_blocked_electronic_signature,
                "bank_accounts_bank_0": bank_accounts_bank_0,
                "bank_accounts_account_type_0": bank_accounts_account_type_0,
                "bank_accounts_agency_0": bank_accounts_agency_0,
                "bank_accounts_account_name_0": bank_accounts_account_name_0,
                "bank_accounts_account_number_0": bank_accounts_account_number_0,
                "bank_accounts_id_0": bank_accounts_id_0,
                "bank_accounts_status_0": bank_accounts_status_0,
                "bank_accounts_bank_1": bank_accounts_bank_1,
                "bank_accounts_account_type_1": bank_accounts_account_type_1,
                "bank_accounts_agency_1": bank_accounts_agency_1,
                "bank_accounts_account_name_1": bank_accounts_account_name_1,
                "bank_accounts_account_number_1": bank_accounts_account_number_1,
                "bank_accounts_id_1": bank_accounts_id_1,
                "bank_accounts_status_1": bank_accounts_status_1,
                "email_validated": email_validated,
                "origin": origin,
                "sinacor_data_cd_con_dep": sinacor_data_cd_con_dep,
                "sinacor_data_in_irsdiv": sinacor_data_in_irsdiv,
                "sinacor_data_in_pess_vinc": sinacor_data_in_pess_vinc,
                "sinacor_data_tp_cliente": sinacor_data_tp_cliente,
                "sinacor_data_tp_pessoa": sinacor_data_tp_pessoa,
                "sinacor_data_tp_investidor": sinacor_data_tp_investidor,
                "sinacor_data_in_situac_cliger": sinacor_data_in_situac_cliger,
                "sinacor_data_cd_cosif": sinacor_data_cd_cosif,
                "sinacor_data_cd_cosif_ci": sinacor_data_cd_cosif_ci,
                "sinacor_data_in_rec_divi": sinacor_data_in_rec_divi,
                "sinacor_data_in_ende": sinacor_data_in_ende,
                "sinacor_data_cod_finl_end1": sinacor_data_cod_finl_end1,
                "sinacor_data_cd_origem": sinacor_data_cd_origem,
                "sinacor_data_in_cart_prop": sinacor_data_in_cart_prop,
                "sinacor_data_in_emite_nota": sinacor_data_in_emite_nota,
                "sinacor_data_in_situac": sinacor_data_in_situac,
                "sinacor_data_pc_corcor_prin": sinacor_data_pc_corcor_prin,
                "sinacor_data_tp_cliente_bol": sinacor_data_tp_cliente_bol,
                "sinacor_data_tp_investidor_bol": sinacor_data_tp_investidor_bol,
                "sinacor_data_ind_pcta": sinacor_data_ind_pcta,
                "sinacor_data_in_emite_nota_cs": sinacor_data_in_emite_nota_cs,
                "sinacor_data_ind_end_vinc_con": sinacor_data_ind_end_vinc_con,
                "sinacor_data_ind_env_email_bvmf": sinacor_data_ind_env_email_bvmf,
                "sinacor_data_tp_cliente_bmf": sinacor_data_tp_cliente_bmf,
                "sinacor_data_ind_opcr_agnt_td": sinacor_data_ind_opcr_agnt_td,
                "sinacor_data_cod_tipo_colt": sinacor_data_cod_tipo_colt,
                "sinacor_data_cod_cep_estr1": sinacor_data_cod_cep_estr1,
                "sinacor_data_uf_estr1": sinacor_data_uf_estr1,
                "sinacor_data_num_class_risc_cmtt": sinacor_data_num_class_risc_cmtt,
                "sinacor_data_desc_risc_cmtt": sinacor_data_desc_risc_cmtt,
                "sinacor_data_pc_corcor_prin_cs": sinacor_data_pc_corcor_prin_cs,
                "sinacor_data_in_tipo_corret_exec_cs": sinacor_data_in_tipo_corret_exec_cs,
                "sinacor_data_num_aut_tra_orde_prcd": sinacor_data_num_aut_tra_orde_prcd,
                "sinacor_data_user_account_block": sinacor_data_user_account_block,
                "us_person": us_person,
                "bureau_validations_cpf": bureau_validations_cpf,
                "bureau_validations_score": bureau_validations_score,
                "external_exchange_requirements_us_is_politically_exposed": external_exchange_requirements_us_is_politically_exposed,
                "external_exchange_requirements_us_politically_exposed_names": external_exchange_requirements_us_politically_exposed_names,
                "external_exchange_requirements_us_is_exchange_member": external_exchange_requirements_us_is_exchange_member,
                "external_exchange_requirements_us_company_ticker_that_user_is_director_of": external_exchange_requirements_us_company_ticker_that_user_is_director_of,
                "external_exchange_requirements_us_is_company_director": external_exchange_requirements_us_is_company_director,
                "external_exchange_requirements_us_is_company_director_of": external_exchange_requirements_us_is_company_director_of,
                "external_exchange_requirements_us_external_fiscal_tax_confirmation": external_exchange_requirements_us_external_fiscal_tax_confirmation,
                "external_exchange_requirements_us_user_employ_company_name": external_exchange_requirements_us_user_employ_company_name,
                "external_exchange_requirements_us_user_employ_position": external_exchange_requirements_us_user_employ_position,
                "external_exchange_requirements_us_user_employ_status": external_exchange_requirements_us_user_employ_status,
                "external_exchange_requirements_us_time_experience": external_exchange_requirements_us_time_experience,
                "external_exchange_requirements_us_w8_confirmation": external_exchange_requirements_us_w8_confirmation,
                "dw": dw,
                "ouro_invest_onboarding_code": ouro_invest_onboarding_code,
                "ouro_invest_status": ouro_invest_status,
                "email_updated_at": email_updated_at,
                "origin_id": origin_id,
                "third_party_sync_picpay": third_party_sync_picpay,
                "is_correlated_to_politically_exposed_person": is_correlated_to_politically_exposed_person,
                "is_politically_exposed_person": is_politically_exposed_person,
                "pld_rating": pld_rating,
                "pld_score": pld_score,
                "sinacor_account_block_status": sinacor_account_block_status,
                "external_exchange_requirements_us_user_employ_company_name": external_exchange_requirements_us_user_employ_company_name,
                "external_exchange_requirements_us_user_employ_position": external_exchange_requirements_us_user_employ_position,
                "external_exchange_requirements_us_user_employ_status": external_exchange_requirements_us_user_employ_status,
                "external_exchange_requirements_us_user_employ_type": external_exchange_requirements_us_user_employ_type,
                "external_exchange_requirements_us_time_experience": external_exchange_requirements_us_time_experience,
                "external_exchange_requirements_us_w8_confirmation": external_exchange_requirements_us_w8_confirmation,
                "dw": dw,
                "ouro_invest_onboarding_code": ouro_invest_onboarding_code,
                "ouro_invest_status": ouro_invest_status,
                "email_updated_at": email_updated_at,
                "origin_id": origin_id,
                "third_party_sync_picpay": third_party_sync_picpay,
                "is_correlated_to_politically_exposed_person": is_correlated_to_politically_exposed_person,
                "is_correlated_to_politically_exposed_person": is_correlated_to_politically_exposed_person,
                "pld_rating": pld_rating,
                "pld_score": pld_score,
                "last_registration_data_update": last_registration_data_update,
                "current_pld_risk_rating_defined_in": current_pld_risk_rating_defined_in,
                "sinacor_account_block_status": sinacor_account_block_status,
                "currentStep": currentStep,
                "steps_import": steps_import,
                "steps_creation": steps_creation,
                "steps_birth": steps_birth,
                "steps_matrimony": steps_matrimony,
                "steps_usPerson": steps_usPerson,
                "steps_document": steps_document,
                "steps_address": steps_address,
                "steps_occupation": steps_occupation,
                "steps_patrimony": steps_patrimony,
                "steps_review": steps_review,
                "expiration_dates_suitability":expiration_dates_suitability,
                "dh_carga": datetime.now()
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
        df.to_csv("oci://bkt-prod-veritas-silver@grilxlpa4rlr/MONGODB/MONGODB_USERS/users.csv",sep=",",index=False,storage_options={"config": config},)
        logging.info("Enviado")

        logging.info("Deleção do arquivo .pem")
        if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem"):
            os.remove(os.path.dirname(os.path.abspath(__file__)) + "/key_par.pem")
            logging.info(".pem deletado")
        else:
            print("Arquivo inexistente")

    load(extract())
