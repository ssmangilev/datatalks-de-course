from datetime import timedelta
import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == 'yellow':
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(
            df['tpep_dropoff_datetime'])
    elif color == 'green':
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(
            df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    print(os.getcwd())
    """Write DataFrame out locally as parquet file"""
    path = Path(f"./data/{color}/{dataset_file}.parquet")
    if not os.path.exists(f"data/{color}"):
        os.makedirs(f"./data/{color}")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_to_gcs(path: Path) -> None:
    """Upload a parquet File to Google Cloud Storage"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path,
                                                    to_path=path)


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The Main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    print(dataset_file)
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_to_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [i for i in range(1, 13)], years: list[int] = [i for i in range(2019, 2022)], color: str = "green"
):
    for year in years:
        for month in months:
            etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    etl_parent_flow(months=[11], years=[2020], color="green")
