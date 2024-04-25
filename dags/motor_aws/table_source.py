from sqlalchemy import create_engine
import pandas as pd
from airflow.models import Variable

def postgres_create_dataframe(fonte):

    db_host = Variable.get("POSTGRES_FUNDS_HOST_PRD")
    db_name = Variable.get("POSTGRES_FUNDS_NAME_PRD")
    db_password = Variable.get("POSTGRES_FUNDS_PASSWORD_PRD")
    db_port = Variable.get("POSTGRES_FUNDS_PORT_PRD")
    db_user = Variable.get("POSTGRES_FUNDS_USER_PRD")

    postgresql_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    conn = create_engine(postgresql_url).connect()

    fonte = "'" + fonte + "'"
    query = 'SELECT "TABELA", "QUERY", "CONTADOR" FROM oci_tables where "FONTE"=' + fonte
    df = pd.read_sql(query, con=conn)

    return df
