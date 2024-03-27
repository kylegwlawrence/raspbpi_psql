from sqlalchemy import create_engine, text
import manage
import pandas as pd

psycopg2_conn = manage.conn_string()
sqlalchemy_conn = manage.conn_string(psycopg2=False)

engine = create_engine(sqlalchemy_conn)

# create the db and schema if they don't exist)
db_name = 'test_db'
schema_name = 'test_schema'
table_name = 'test_table'
manage.create_db(db_name, psycopg2_conn)
manage.create_schema(schema_name, psycopg2_conn)
manage.drop_table(schema_name, table_name, psycopg2_conn)

# create table when loading a df with sqlalchemy
# Initialize data to lists.
data = [{'a': 1, 'b': 2, 'c': 3},
        {'a': 10, 'b': 20, 'c': 30}]
# import some data
df = pd.DataFrame(data)
df.to_sql(name=table_name, con=engine, schema=schema_name, index=False)

manage.table_exists(schema_name, table_name, psycopg2_conn)

df_v = pd.read_sql_table(table_name, sqlalchemy_conn, schema_name)
print(df_v)


# on cluster use localhost and port 32345
# open the port from docker container to local system - maybe?