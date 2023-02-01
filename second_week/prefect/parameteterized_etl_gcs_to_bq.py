from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task
import pandas as pd
from pathlib import Path


@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(gcs_path)


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Transform Parquet file to DataFramepython3 """
    df = pd.read_parquet(path)
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    gcp_creds_block = GcpCredentials.load("de-gcp-creds")
    df.to_gbq(
        destination_table='trips_data_all.rides_yellow',
        project_id='instant-pivot-375017',
        credentials=gcp_creds_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq(color, year, month) -> None:
    """The Main ETL flow to load data into Big Query"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    path = extract_from_gcs(color, year, month)
    print(path)
    df = transform(path)
    write_bq(df)
    return len(df)


@flow(log_prints=True)
def parent_etl(color: str = "yellow", months: list[int] = [1, 2], years: list[int] = [i for i in range(2019, 2021)]):
    sum_of_rows = 0
    for year in years:
        for month in months:
            sum_of_rows += etl_gcs_to_bq(color, year, month)
    print(sum_of_rows)


if __name__ == '__main__':
    color = "yellow"
    months = [2, 3]
    years = [2019]
    parent_etl(color, months, years)
