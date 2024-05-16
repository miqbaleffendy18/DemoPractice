#Cleaned up version of data-loading.ipynb
import argparse, os, sys
from time import time
import pandas as pd 
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main():

    path = './'
    file_name = 'yellow_tripdata_2021-01.parquet'
    filepath = os.path.join(path, file_name)
    tb = 'yellow_taxi_data'

    # Create SQL engine
    engine = create_engine(f'postgresql://root:root@localhost:5432/ny_taxi')

    # Read file based on csv or parquet
    if '.csv' in file_name:
        df = pd.read_csv(filepath, nrows=10)
        df_iter = pd.read_csv(filepath, iterator=True, chunksize=100000)
    elif '.parquet' in file_name:
        file = pq.ParquetFile(filepath)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else: 
        print('Error. Only .csv or .parquet files allowed.')
        sys.exit()


    # Create the table
    df.head(0).to_sql(name=tb, con=engine, if_exists='replace')


    # Insert values
    t_start = time()
    count = 0
    for batch in df_iter:
        count+=1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f'inserting batch {count}...')

        b_start = time()
        batch_df.to_sql(name=tb, con=engine, if_exists='append')
        b_end = time()

        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
        
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')    



if __name__ == '__main__':
    main()




