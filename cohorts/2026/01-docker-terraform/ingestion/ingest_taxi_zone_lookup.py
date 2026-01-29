import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import pyarrow.parquet as pq
import fsspec

first = True
table_name = 'taxi_zone_lookup'

wget_zones = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

host='pg' # when running inside docker-compose, use the service name as host

engine = create_engine(f'postgresql://admin:password@{host}:5432/taxi')

df = pd.read_csv(wget_zones)

df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
print(f'{table_name} created.')

df.to_sql(name=table_name, con=engine, if_exists='append')
print(f"{table_name} appended with {len(df)} rows")
