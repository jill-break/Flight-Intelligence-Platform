"""
RAW Ingestion DAG — File Mover
================================
Monitors the local data/ folder for new flight CSVs and uploads
them to MinIO's raw-data bucket. Deletes the local copy after upload.

This DAG is DECOUPLED from processing — it only moves files.
The flight_analytics_pipeline DAG handles validation and transformation.

Schedule: Every 2 minutes
"""

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import glob
import os
import logging

logger = logging.getLogger('raw_ingestion_dag')

default_args = {
    'owner': 'senior_de',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

MINIO_CONN_ID = 'minio_conn'
BUCKET = 'raw-data'
LOCAL_DATA_DIR = '/opt/airflow/data'


def check_for_files():
    """Return True if any flights_*.csv exist in the data folder."""
    files = glob.glob(os.path.join(LOCAL_DATA_DIR, 'flights_*.csv'))
    logger.info(f"Scanning {LOCAL_DATA_DIR} — found {len(files)} CSV file(s)")
    return len(files) > 0


def upload_files_to_minio(**kwargs):
    """
    Find all flights_*.csv in the local data/ folder,
    upload each to MinIO raw-data bucket, then delete the local copy.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    csv_files = glob.glob(os.path.join(LOCAL_DATA_DIR, 'flights_*.csv'))
    if not csv_files:
        logger.warning("No new CSV files found in data/ folder.")
        return

    uploaded = 0
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        logger.info(f"Uploading {filename} to MinIO bucket '{BUCKET}'...")

        s3_hook.load_file(
            filename=filepath,
            key=filename,
            bucket_name=BUCKET,
            replace=True,
        )

        # Delete local copy after successful upload
        os.remove(filepath)
        uploaded += 1
        logger.info(f"Uploaded and removed local copy: {filename}")

    logger.info(f"Ingestion complete: {uploaded} file(s) uploaded to MinIO.")


with DAG(
    'raw_ingestion_pipeline',
    default_args=default_args,
    description='File Mover: local data/ -> MinIO raw-data bucket',
    schedule_interval='*/2 * * * *', # every 2 minutes
    catchup=False,
    tags=['ingestion', 'file-mover', 'flight-data'],
) as dag:

    # 1. Wait for at least one file to appear
    wait_for_local_file = PythonSensor(
        task_id='wait_for_local_file',
        python_callable=check_for_files,
        poke_interval=30,
        timeout=300,
        mode='reschedule',
    )

    # 2. Upload all found files to MinIO
    upload_to_minio = PythonOperator(
        task_id='upload_files_to_minio',
        python_callable=upload_files_to_minio,
    )

    wait_for_local_file >> upload_to_minio
