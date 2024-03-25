from sqlalchemy import create_engine, text
import json

with open('app/server_params.json') as f:
    params = json.load(f)

engine = create_engine(f"postgresql://{params['user']}:{params['pw']}@{params['host']}:{params['port']}/{params['db']}")
with engine.connect() as conn:
    t = text("""SELECT datname FROM pg_database WHERE datistemplate = false""")
    t_result = conn.execute(t).fetchall()
    print(f"Databases: {t_result}")
    r = text("""SELECT table_schema,table_name FROM information_schema.tables ORDER BY table_schema,table_name;""")
    r_result = conn.execute(r).fetchall()
    print(f"Tables: {r_result}")
    u = text("SELECT * from pg_catalog.pg_user;")
    u_result = conn.execute(u).fetchall()
    print(f"pg_catalog.pg_user: {u_result}")