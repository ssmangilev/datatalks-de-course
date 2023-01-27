import argparse
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine
from prefect import flow, task


@task()
def extract_data():
    pass



@task(log_prints=True, retries=3)
def main(params):   
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    databasename = params.databasename
    table_name = params.table_name
    url = params.url
    zones_url = params.zones_url
    file_name = 'output.csv.gz'
    zones_file_name = 'zones.csv'
    os.system(f"wget {url} -O {file_name}")
    os.system(f"wget {zones_url} -O {zones_file_name}")
    df = pd.read_csv(file_name)
    print(df)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{databasename}")
    engine.connect()
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))
    df.to_sql(con=engine, name=table_name, if_exists="replace")
    zones_df  = pd.read_csv(zones_file_name)
    print(pd.io.sql.get_schema(zones_df, name="zones", con=engine))
    zones_df.to_sql(con=engine, name="zones", if_exists="replace")

@flow(name="Ingest Data")
def main_flow():
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest Parquet data to Postgres")
    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the parquet

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--databasename', help='databasename for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')
    parser.add_argument('--zones_url', help='url of the csv file with zones')

    args = parser.parse_args()
    main(args)

