import os
import logging
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.utils.dates import days_ago

from google.cloud import storage

from pyarrow import csv, parquet, Table
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_url = f"https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
filename = "zones.csv.gz"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


@dag(
    schedule="@once",
    start_date=pendulum.now(),
    catchup=False,
    tags=["ny_zones"]
)
def ny_taxi_zones_dag_v3():
    """
    ### DAG for an upload NY taxi zones from github to GCS
    This is a simple data pipeline which demonstrated how simple 
    is uploading a data from csv to GCS
    """

    @task
    def transform_csv_to_parquet(src: str) -> str:
        """The task transforms .csv file to .parquet
        and saves it on the disk"""
        if not src.endswith('.csv.gz'):
            logging.error(
                "Can only accept source files in CSV format, for the moment")
            return
        parqeut_src = src.replace('.csv', '.parquet')
        table = csv.read_csv(src)
        parquet.write_table(table, parqeut_src)
        return parqeut_src

    @task
    def upload_to_gcs(local_file: str) -> str:
        client = storage.Client()
        bucket = client.bucket(BUCKET)

        blob = bucket.blob(local_file)
        blob.upload_from_filename(f"{path_to_local_home}/{local_file}")
        return local_file

    extract = BashOperator(
        task_id="download_ytd_dataset_from_web",
        bash_command=f"wget {dataset_url} -O {path_to_local_home}/{filename}"
    )
    transform = transform_csv_to_parquet(filename)
    load = upload_to_gcs(transform)

    extract >> transform >> load


ny_taxi_zones_dag_v3()
