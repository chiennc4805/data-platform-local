import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    parquet_file_name = "yellow_tripdata_2021-01.parquet"
    
    os.system(f"wget {url} -O {parquet_file_name}")
    
    df = pd.read_parquet(f"{parquet_file_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    print(pd.io.sql.get_schema(df, name=f"{table_name}", con=engine))

    df.to_sql(f"{table_name}", con=engine, if_exists="replace")
    

if __name__ == '__main__':
    parser = argparse. ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of the table where we will write the results to")
    parser.add_argument("--url", help="url of parquet file")

    args = parser.parse_args ()
    
    main(args)

