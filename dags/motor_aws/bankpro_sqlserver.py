import pandas as pd
import pyodbc
from airflow.models import Variable

class Bankpro_class:
    def __init__(self) -> None:
        pass

    def create_connection(self):
        servidor = Variable.get("SQL_SERVER_PMS_SERVER_PRD")
        database = Variable.get("SQL_SERVER_PMS_DATABASE_PRD")
        usuario = Variable.get("SQL_SERVER_PMS_USER_PRD")
        senha = Variable.get("SQL_SERVER_PMS_PASSWORD_PRD")
        connection_bankpro = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + servidor
            + ";DATABASE="
            + database
            + ";UID="
            + usuario
            + ";PWD="
            + senha
        )

        return connection_bankpro

    def execute_query(self, query, connection):
        df = pd.read_sql(query, connection)

        return df
    
    def get_rows_count(self, query, connection):
        df = pd.read_sql(query, connection)

        return df

    def end_connection(self, connection):
        connection.close()
