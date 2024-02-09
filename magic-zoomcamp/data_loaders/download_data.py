import io
import pandas as pd
import requests
from typing import List

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def create_urls(year: int, months: List[str]) -> List[str]:
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
    urls = []
    
    for month in months:
        url = f"{base_url}/green_tripdata_{year}-{month}.csv.gz"
        urls.append(url)
        
    return urls

def load_data_from_api(url):
    """
    Template for loading data from API
    """

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'RatecodeID':pd.Int64Dtype(),
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'ehail_fee':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'payment_type': pd.Int64Dtype(),
                    'trip_type': pd.Int64Dtype(),
                    'congestion_surcharge':float
                }

    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    print(f"Getting data from {url}")

    return pd.read_csv(
        url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates
        )

@data_loader
def download_data():
    
    year = 2020
    months = [10, 11, 12]
    urls = create_urls(year, months)
    dfs = []

    for url in urls:
        df = load_data_from_api(url)
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
