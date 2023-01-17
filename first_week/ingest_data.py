import argparse
import os

import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine

def main(params):   
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    databasename = params.databasename
    table_name = params.table_name
    url = params.url
    file_name = 'output.parquet'
    os.system(f"wget {url} -O {file_name}")
    df = pq.read_table(source=file_name).to_pandas()
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{databasename}")
    engine.connect()
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))
    df.to_sql(con=engine, name=table_name, if_exists="replace")


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

    args = parser.parse_args()
    main(args)

