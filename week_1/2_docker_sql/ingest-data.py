#!/usr/bin/env python
# coding: utf-8
import argparse
from time import time
import os

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import psycopg2

parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')


#user, password, host, port, database name, table name,
#url of the csv
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    table_name1 = params.table_name1
    url = params.url
    url1 = params.url1
    gz_name = 'green.csv.gz'
    csv_name = "taxi_zone.csv"


    # download the csv
    os.system(f"wget {url} -O {gz_name}")
    os.system(f"wget {url1} -O {csv_name}")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df1= pd.read_csv(csv_name)
    df1.to_sql(name = table_name1, con = engine, if_exists='replace')

    df = pd.read_csv(gz_name, compression='gzip')
   
    df_iter = pd.read_csv(gz_name, compression='gzip', iterator = True, chunksize = 100000)

    #df = next(df_iter)

    #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    while True:
        t_start = time()
    
        df = next(df_iter)
    
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
        df.to_sql(name =table_name, con = engine, if_exists = 'append')
    
        t_end = time()
    
        print('inserted another chunk...., took %.3f second' % (t_end - t_start))

if __name__ == '__main__':    

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we will write green taxi results to')
    parser.add_argument('--table_name1', help='name of table where csv result will be written to')
    parser.add_argument('--url', help='url of the parquet file')
    parser.add_argument('--url1', help='url of the csv file')


    args = parser.parse_args()

    main(args)


