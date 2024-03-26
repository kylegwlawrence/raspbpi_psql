from manage import create_db, drop_db, conn_string, create_schema, drop_schema, create_table, drop_table, table_exists
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

class Database():
    """A class to operate on a database in postgres"""
    def __init__(self, name) -> None:
        self.name = name
    def create(self):
        create_db(self.name, conn_string())
    def drop(self):
        drop_db(self.name, conn_string())
    def exists(self):
        """Return True if the database exists and False if it does not"""
        is_exist = f"SELECT datname FROM pg_database WHERE datistemplate = false and datname='{self.name}'"
        conn = psycopg2.connect(conn_string())
        conn.autocommit = True
        with conn:
            with conn.cursor() as curs:
                curs.execute(is_exist)
                exist_result = curs.fetchall()
        if not exist_result:
            return False
        else:
            return True

class Schema():
    """A class to operate on a schema in postgres"""
    def __init__(self, name) -> None:
        self.name = name
    def create(self):
        create_schema(self.name, conn_string())
    def drop(self):
        drop_schema(self.name, conn_string())
    def exists(self):
        is_exist = f"SELECT nspname FROM pg_catalog.pg_namespace WHERE LOWER(nspname) = LOWER('{self}')"
        conn = psycopg2.connect(conn_string())
        conn.autocommit = True
        with conn:
            with conn.cursor() as curs:
                curs.execute(is_exist)
                exist_result = curs.fetchall()
        if not exist_result:
            return False
        else:
            return True

class Table():
    """A class to operate on a table in postgres"""
    def __init__(self, schema, name) -> None:
        self.schema = schema
        self.name = name
    def create(self):
        create_table(self.schema, self.name, conn_string())
    def drop(self):
        drop_table(self.schema, self.name, conn_string())
    def exists(self):
        table_exists(self.schema, self.name, conn_string())

class SelectQuery():
    """A class to operate on a select query in postgres"""
    def __init__(self, query) -> None:
        self.query = query
    def to_df(self):
        engine = create_engine(conn_string(psycopg2=False))
        df = pd.read_sql(self.query, con = engine)
        return df
    def to_table(self, schema, name):
        """Loads the result from the select query into a postgres table"""
        table = Table(self, schema, name)