from manage import create_db, drop_db, conn_string, create_schema, drop_schema, create_table, drop_table, table_exists
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import json

class ConnectionString:
    """
    Connects to the raspberrypi3.local postgres server

        Parameters:
            json_file (json): json file with the connection parameters
            psycopg2 (bool): if True returns a string to use in psycoopg2, else returns a string to use in sqlalchemy
        
        Returns:
            connection_string (str): the connection string to pass to the cursor
    """
    def __init__(self, json_file:str='app/server_params.json', use_psycopg2:bool=True) -> str:
        self.json_file = json_file
        self.use_psycopg2 = use_psycopg2
    
    def connect(self):
        """Connect to the database via either psycopg2 or sqlalchemy"""
        # read in json
        with open(self.json_file) as f:
            params = json.load(f)
        if self.use_psycopg2:
            connection_string = f"""host='{params["host"]}' port='{params["port"]}' dbname='{params["db"]}' user='{params["user"]}' password='{params["pw"]}'"""
            conn = 'psycopg2 connection'
        else:
            connection_string = f"postgresql://{params['user']}:{params['pw']}@{params['host']}:{params['port']}/{params['db']}"
            conn = 'sqlalchmey engine'
        return conn
        
class Database:
    """A class to operate on a database in postgres"""
    def __init__(self, name) -> None:
        self.name = name
        self.connection_string = ConnectionString()
    def create(self):
        create_db(self.name, self.connection_string)
    def drop(self):
        drop_db(self.name, self.connection_string)
    def exists(self):
        """Return True if the database exists and False if it does not"""
        is_exist = f"SELECT datname FROM pg_database WHERE datistemplate = false and datname='{self.name}'"
        conn = psycopg2.connect(self.connection_string)
        conn.autocommit = True
        with conn:
            with conn.cursor() as curs:
                curs.execute(is_exist)
                exist_result = curs.fetchall()
        if not exist_result:
            return False
        else:
            return True

class Schema:
    """A class to operate on a schema in postgres"""
    def __init__(self, name) -> None:
        self.name = name
        self.connection_string = ConnectionString()
    def create(self):
        create_schema(self.name, self.connection_string)
    def drop(self):
        drop_schema(self.name, self.connection_string)
    def exists(self):
        is_exist = f"SELECT nspname FROM pg_catalog.pg_namespace WHERE LOWER(nspname) = LOWER('{self}')"
        conn = psycopg2.connect(self.connection_string)
        conn.autocommit = True
        with conn:
            with conn.cursor() as curs:
                curs.execute(is_exist)
                exist_result = curs.fetchall()
        if not exist_result:
            return False
        else:
            return True

class Table:
    """A class to operate on a table in postgres"""
    def __init__(self, schema, name) -> None:
        self.schema = schema
        self.name = name
        self.connection_string = ConnectionString()
    def create(self):
        create_table(self.schema, self.name, self.connection_string)
    def drop(self):
        drop_table(self.schema, self.name, self.connection_string)
    def exists(self):
        table_exists(self.schema, self.name, self.connection_string)
    def alter(self):
        sql = f"""ALTER {self.schema}.{self.name} RENAME"""
        return 'placeholder'
    def to_df(self):
        """Grabs data from postgres and loads it to a dataframe. Can be a table or a select query."""
        #engine = create_engine(conn_string(psycopg2=False))
        self.df = pd.read_sql(self.query, con = engine)
        return self.df
    def load_table(self):
        """Loads the result from the select query into a postgres table"""
        # loads the table with either a sql select query or a dataframe. 
        # the former uses psycopg2, the latter uses sqlalchemy
        pass 

class SelectQuery:
    """A class to operate on a select query in postgres"""
    def __init__(self, query) -> None:
        self.query = query

class TestData:
    """Contains fake data to test connection and writing to postgres"""
    def __init__(self) -> None:
        pass