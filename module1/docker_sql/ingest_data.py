


import pandas as pd
import sqlalchemy  # Import the module
from sqlalchemy import create_engine  
from time import time
import argparse
import os 




# user 
# password
# host
# port 
# database 
# table 
# url   


def main(params):
    user = params.user 
    password = params.password
    host = params.host
    port = params.port 
    database = params.database 
    table = params.table 
    url = params.url   

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'



    # download the csv
    os.system(f"wget {url} -O {csv_name}")   
 
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    df_iter = pd.read_csv(csv_name, compression='gzip',parse_dates=['tpep_pickup_datetime' , 'tpep_dropoff_datetime'], iterator=True ,chunksize=100000,low_memory=False)
    df = next(df_iter)
    df.head(n=0).to_sql(con =engine , name = table , if_exists='replace')
    chunk_count = 1
    while True :
        try:
            t_start = time()
            df.to_sql(con=engine, name= table, if_exists='append')
            t_end = time()
            print(f'Inserted another chunk {chunk_count}, took {t_end - t_start:.3f} seconds')
            chunk_count += 1
            df = next(df_iter)
        except StopIteration:
            print("All chunks have been processed.")
            break

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='ingest csv data to postgres')
    parser.add_argument('--user' , required=True ,help ='user name for postgres')
    parser.add_argument('--password' , required=True ,help ='pass name for postgres')
    parser.add_argument('--host' , required=True ,help ='host name for postgres')
    parser.add_argument('--port' , required=True ,help ='port name for postgres')
    parser.add_argument('--database' , required=True ,help ='database name for postgres')
    parser.add_argument('--table' , required=True ,help ='table name for postgres')
    parser.add_argument('--url' , required=True ,help ='file url for csv')

    args = parser.parse_args()

    main(args)




