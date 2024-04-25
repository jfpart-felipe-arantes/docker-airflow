import oracledb
import pandas as pd
from airflow.models import Variable

class Sinacor_class():
    def __init__(self) -> None:
        pass

    def create_connection(self):
        connection = oracledb.connect(
            user=Variable.get("ORACLE_USERNAME_PRD"),
            password=Variable.get("ORACLE_PASSWORD_PRD"),
            dsn=Variable.get("ORACLE_DSN_PRD")
        )

        return connection
    
    def execute_query(self, query, connection):

        df = pd.read_sql(query, connection)
        
        return df

    def get_rows_count(self, query, connection):
        df = pd.read_sql(query, connection)

        return df
    
    def end_connection(self, connection):
        
        connection.close()


