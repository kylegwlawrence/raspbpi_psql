from sqlalchemy import create_engine
import pandas as pd
import json
import argparse

def select_query(args):
    """Return results of a select query to a dataframe"""

    password =  args.pw
    sql =       args.sql

    # read in json
    json_file='app/server_params.json'
    try:
        with open(json_file) as f:
            params = json.load(f)
    except:
        with open(json_file.replace('app/','')) as f:
            params = json.load(f)
    
    # read in sql file if it is passed
    if sql.endswith(".sql"):
        try:
            with open(sql, 'r') as f:
                sql = f.read()
        except:
            with open(sql.replace('app/',''),'r') as f:
                sql = f.read()

    user = params["user"]
    host = params["host"]
    port = params["port"]
    database = params["dev_db"]
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    df = pd.read_sql(sql, con=engine)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    print(df)
    return df

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Query postgres and return a dataframe. Wrap a sql statement in double quotes.')
    parser.add_argument('--pw', required=True, help='password for postgres')
    parser.add_argument('--sql', required=True, help='select statement or file path to a sql file containing a select statement')

    args = parser.parse_args()

    select_query(args)