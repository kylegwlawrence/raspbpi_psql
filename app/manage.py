# functions to create, alter, delete, etc pg objects
# such as databases, schemas, tables, views, users, etc

import psycopg2
import json

def conn_string(json_file='app/server_params.json', psycopg2=True) -> str:
    """
    Connects to the raspberrypi3.local postgres server

        Parameters:
            json_file (json): json file with the connection parameters
            psycopg2 (bool): if True returns a string to use in psycoopg2, else returns a string to use in sqlalchemy
        
        Returns:
            connection_string (str): the connection string to pass to the cursor
    """
    # read in json
    with open(json_file) as f:
        params = json.load(f)
    if psycopg2:
        connection_string = f"""host='{params["host"]}' port='{params["port"]}' dbname='{params["db"]}' user='{params["user"]}' password='{params["pw"]}'"""
    else:
        connection_string = f"postgresql://{params['user']}:{params['pw']}@{params['host']}:{params['port']}/{params['db']}"
    return connection_string

def create_db(new_db_name, conn_string) -> None:
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

def drop_db(db_name, conn_string) -> None:
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

def create_schema(new_schema_name, conn_string) -> None:

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

def drop_schema(schema_name, conn_string) -> None:
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

def create_table(schema_name, new_table_name, conn_string) -> None:
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

def drop_table(schema_name, table_name, conn_string) -> None:
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

def table_exists(schema_name, table_name, conn_string):
    is_exist = f"""SELECT table_name
    FROM
        information_schema.tables
    WHERE
        table_name = '{table_name}'
    AND
        table_schema = '{schema_name}';"""
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    curs = conn.cursor()
    # verify it does not exist
    curs.execute(is_exist)
    exist_result = curs.fetchall()
    if not exist_result:
        print(f'Table {schema_name}.{table_name} does not exist')
    elif exist_result:
        print(f'Verified that table {schema_name}.{table_name} exists')
    conn.close()

if __name__ == '__main__':
    db = 'test_db'
    schema = 'test_schema'
    table = 'test_table'
    conn = conn_string()
    create_schema(schema, conn)
    #drop_schema('test_schema', conn)
    create_table(schema,table, conn)
    #drop_table('test_schema', 'test_table', conn)
    verify_table_loaded(schema, table, conn)