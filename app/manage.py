# functions to create, alter, delete, etc pg objects
# such as databases, schemas, tables, views, users, etc

import psycopg2
import json

def conn_string(params_json):
    """
    Connects to the raspberrypi3.local postgres server

        Parameters:
            params_json (json): json file with the connection parameters
        
        Returns:
            connection_string (str): the connection string to pass to the cursor
    """
    # read in json
    with open(params_json) as f:
        params = json.load(f)
    connection_string = f"""host='{params["host"]}' port='{params["port"]}' dbname='{params["db"]}' user='{params["user"]}' password='{params["pw"]}'"""
    return connection_string

def create_db(new_db_name, conn_string):
    """
    Creates a new database on the postgres server
    """
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    curs = conn.cursor()
    # sql to check if the db already exists
    is_exist = f"SELECT datname FROM pg_database WHERE datistemplate = false and datname='{new_db_name}'"
    with conn:
        with conn.cursor() as curs:
            curs.execute(is_exist)
            exist_result = curs.fetchall()
    if not exist_result:
        # create the database
        print(f'{new_db_name} does not exist, creating it...')
        # sql to create the db
        sql = f"CREATE DATABASE {new_db_name};"
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        curs = conn.cursor()
        curs.execute(sql)
        conn.commit()

        # verify it exists
        curs.execute(is_exist)
        exist_result = curs.fetchall()
        if exist_result[0][0]==new_db_name:
            print(f'Database {new_db_name} has been created')
        conn.close()
    else:
        print(f'Database {new_db_name} already exists...')

def drop_db(db_name, conn_string):
    """
    Drops an existing database on the postgres server
    """
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    # sql to check if the db already exists
    is_exist = f"SELECT datname FROM pg_database WHERE datistemplate = false and datname='{db_name}'"
    with conn:
        with conn.cursor() as curs:
            curs.execute(is_exist)
            exist_result = curs.fetchall()
    if not exist_result:
        print(f'Database {db_name} cannot be droppped, it does not exist...')
    else:
        sql_drop = f"DROP database {db_name};"
        curs = conn.cursor()
        curs.execute(sql_drop)
        conn.commit()
        # verify it does not exist
        curs.execute(is_exist)
        exist_result = curs.fetchall()
        if not exist_result:
            print(f'Database {db_name} has been dropped')
        conn.close()

def create_schema(new_schema_name, conn_string):

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    curs = conn.cursor()
    # sql to check if the schema already exists
    is_exist = f"SELECT nspname FROM pg_catalog.pg_namespace WHERE LOWER(nspname) = LOWER('{new_schema_name}')"
    with conn:
        with conn.cursor() as curs:
            curs.execute(is_exist)
            exist_result = curs.fetchall()
    if not exist_result:
        # create the schema
        print(f'{new_schema_name} does not exist, creating it...')
        # sql to create the schema
        sql = f"CREATE SCHEMA {new_schema_name};"
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        curs = conn.cursor()
        curs.execute(sql)
        conn.commit()

        # verify it exists
        curs.execute(is_exist)
        exist_result = curs.fetchall()
        if exist_result[0][0]==new_schema_name:
            print(f'Schema {new_schema_name} has been created')
        conn.close()
    else:
        print(f'Schema {new_schema_name} already exists...')

def drop_schema(schema_name, conn_string):
    """
    Drops an existing schema on the postgres server
    """
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    # sql to check if the schema already exists
    is_exist = f"SELECT nspname FROM pg_catalog.pg_namespace WHERE LOWER(nspname) = LOWER('{schema_name}')"
    with conn:
        with conn.cursor() as curs:
            curs.execute(is_exist)
            exist_result = curs.fetchall()
    if not exist_result:
        print(f'Schema {schema_name} cannot be droppped, it does not exist...')
    else:
        sql_drop = f"DROP SCHEMA {schema_name};"
        curs = conn.cursor()
        curs.execute(sql_drop)
        conn.commit()
        # verify it does not exist
        curs.execute(is_exist)
        exist_result = curs.fetchall()
        if not exist_result:
            print(f'Schema {schema_name} has been dropped')
        conn.close()

def create_table(schema_name, new_table_name, conn_string):
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    is_exist = f"""SELECT table_name
    FROM
        information_schema.tables
    WHERE
        table_name = '{new_table_name}'
    AND
        table_schema = '{schema_name}';"""
    with conn:
        with conn.cursor() as curs:
            curs.execute(is_exist)
            exist_result = curs.fetchall()
    if not exist_result:
        # sql to create the table
        sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{new_table_name} ();"
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        curs = conn.cursor()
        curs.execute(sql)
        conn.commit()
        # verify it exists
        curs.execute(is_exist)
        exist_result = curs.fetchall()
        if exist_result[0][0]==new_table_name:
            print(f'TABLE {schema_name}.{new_table_name} has been created')
    else:
        print(f'Table {schema_name}.{new_table_name} already exists...')
    conn.close()

def drop_table(schema_name, table_name, conn_string):
    """
    Drops an existing table on the postgres server
    """
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    # sql to check if the schema already exists
    is_exist = f"""SELECT table_name
    FROM
        information_schema.tables
    WHERE
        table_name = '{table_name}'
    AND
        table_schema = '{schema_name}';"""
    with conn:
        with conn.cursor() as curs:
            curs.execute(is_exist)
            exist_result = curs.fetchall()
    if not exist_result:
        print(f'Table {schema_name}.{table_name} cannot be droppped, it does not exist...')
    else:
        sql_drop = f"DROP TABLE {schema_name}.{table_name} CASCADE;"
        curs = conn.cursor()
        curs.execute(sql_drop)
        conn.commit()
        # verify it does not exist
        curs.execute(is_exist)
        exist_result = curs.fetchall()
        if not exist_result:
            print(f'Table {schema_name}.{table_name} has been dropped')
        conn.close()

if __name__ == '__main__':
    conn = conn_string('app/server_params.json')
    create_schema('test_schema', conn)
    #drop_schema('test_schema', conn)
    create_table('test_schema','test_table', conn)
    drop_table('test_schema', 'test_table', conn)