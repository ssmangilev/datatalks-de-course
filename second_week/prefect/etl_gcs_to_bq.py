from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(gcs_path)


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(
        f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(
        f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    gcp_creds_block = GcpCredentials.load("de-gcp-creds")
    df.to_gbq(
        destination_table='trips_data_all.rides',
        project_id='instant-pivot-375017',
        credentials=gcp_creds_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq() -> None:
    """The Main ETL flow to load data into Big Query"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    path = extract_from_gcs(color, year, month)
    print(path)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    etl_gcs_to_bq()
