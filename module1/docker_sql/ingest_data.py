import pandas as pd
import sqlalchemy  # Import SQLAlchemy for database connection
from sqlalchemy import create_engine  
from time import time
import argparse
import os 


def main(params):
    """
    Main function to download a CSV file, read it in chunks, and insert the data into a PostgreSQL database.
    """
    # Extract parameters from command-line arguments
    user = params.user 
    password = params.password
    host = params.host
    port = params.port 
    database = params.database 
    table = params.table 
    url = params.url   

    # Determine the filename for the downloaded CSV
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # Download the CSV file from the given URL using wget
    os.system(f"wget {url} -O {csv_name}")   

    # Create a connection to the PostgreSQL database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    
    # Read the CSV file in chunks to handle large datasets efficiently
    df_iter = pd.read_csv(csv_name, compression='gzip', parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
                           iterator=True, chunksize=100000, low_memory=False)
    
    # Read the first chunk
    df = next(df_iter)

    # Create the table schema in the database (if it doesn't exist)
    df.head(n=0).to_sql(con=engine, name=table, if_exists='replace')

    chunk_count = 1  # Counter to track the number of chunks processed

    # Process each chunk and insert it into the database
    while True:
        try:
            t_start = time()
            df.to_sql(con=engine, name=table, if_exists='append')  # Append the chunk to the table
            t_end = time()
            print(f'Inserted another chunk {chunk_count}, took {t_end - t_start:.3f} seconds')
            chunk_count += 1
            df = next(df_iter)  # Load the next chunk
        except StopIteration:
            print("All chunks have been processed.")
            break  # Exit loop when no more chunks are left


if __name__ == '__main__':
    """
    Entry point of the script. Uses argparse to parse command-line arguments and pass them to the main function.
    """

    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL database')
    
    # Define command-line arguments
    parser.add_argument('--user', required=True, help='Postgres username')
    parser.add_argument('--password', required=True, help='Postgres password')
    parser.add_argument('--host', required=True, help='Postgres host')
    parser.add_argument('--port', required=True, help='Postgres port')
    parser.add_argument('--database', required=True, help='Postgres database name')
    parser.add_argument('--table', required=True, help='Target table name in Postgres')
    parser.add_argument('--url', required=True, help='URL of the CSV file to be ingested')

    # Parse arguments from the command line
    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args)
