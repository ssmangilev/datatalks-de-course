import os
import logging
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago

from google.cloud import storage

from pyarrow import csv, parquet, Table
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2019, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    tags=["fhv_taxi_data"]
)
def fhv_taxi_data_dag_in_cgs_v6():
    """
    ### DAG for an upload FHV data from github to GCS
    This is a simple data pipeline which demonstrated how simple 
    is uploading a data from csv to GCS
    """

    @task()
    def build_csv_filename() -> str:
        """
        The task gets a date of execution from DAGs context
        and builds a filename, after that downloads the file
        from url
        """
        context = get_current_context()
        execution_date = context.get('dag_run').execution_date
        month = f"{execution_date.month:02}"
        filename = f"fhv_tripdata_{execution_date.year}-{month}.csv.gz"
        return filename

    filename = build_csv_filename()
    extract = BashOperator(
        task_id="download_ytd_dataset_from_web",
        bash_command=f"wget {dataset_url}{filename} -O {path_to_local_home}/{filename}"
    )
    load = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=f"{path_to_local_home}/{filename}",
        dst=f"data/fhv/{filename}",
        bucket=BUCKET,
    )

    filename >> extract >> load


fhv_taxi_data_dag_in_cgs_v6()
