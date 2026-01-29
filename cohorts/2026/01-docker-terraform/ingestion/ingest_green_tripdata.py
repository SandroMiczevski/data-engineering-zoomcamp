import sys
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import pyarrow.parquet as pq
import fsspec

ingest_year = 2025
ingest_month = 11
first = True

wget_tripdata = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{ingest_year}-{ingest_month:02d}.parquet'

host='pg' # when running inside docker-compose, use the service name as host

engine = create_engine(f'postgresql://admin:password@{host}:5432/taxi')

def ingest_to_postgres(df, f_first=False):
    table_name = f'trip_data_{ingest_year}_{ingest_month:02d}'

    if f_first:
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        print(f"{table_name} created")
    else:
        df.to_sql(name=table_name, con=engine, if_exists='append')
        print(f"{table_name} appended with {len(df)} rows")

def read_parquet_chunks_from_uri(uri, chunk_size=10000):
    # fsspec handles opening the URI (http, s3, gs, etc.)
    with fsspec.open(uri, "rb") as f:
        # Create a ParquetFile object from the file-like object
        parquet_file = pq.ParquetFile(f)

        # Iterate over batches (RecordBatches)
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            # Process each batch (e.g., convert to pandas DataFrame)
            yield batch.to_pandas()

for i, chunk_df in enumerate(read_parquet_chunks_from_uri(wget_tripdata, chunk_size=50000)):
    if first:
        ingest_to_postgres(chunk_df, first)
        first = False

    print(f"Processing chunk {i}: shape {chunk_df.shape}")    
    ingest_to_postgres(chunk_df)
    # Perform your processing on the chunk_df here

