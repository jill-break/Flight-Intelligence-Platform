"""
Flight Analytics Pipeline DAG — Processing Engine
====================================================
Waits for files in MinIO -> Validates with Pandera -> Transforms with Spark -> Archives

This DAG is DECOUPLED from ingestion — it only processes files already in MinIO.
Files arrive via the raw_ingestion_pipeline DAG (local uploads) or other sources.

Task flow:
  wait_for_file -> validate_with_pandera -> process_flight_data_spark -> archive_processed_files

Data Quality Gate:
  - Pandera validates every CSV against FlightSchema (lazy=True)
  - If ANY file fails -> DAG fails, Spark never runs
  - Bad files are quarantined to quarantine/ in MinIO
"""

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import pandas as pd
import pandera
import io
import logging

from spark.schemas.flight_schema import FlightSchema

logger = logging.getLogger('flight_analytics_pipeline')

# Default Args
default_args = {
    'owner': 'senior_de',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['data-team@flightintel.io'],
}

MINIO_CONN_ID = 'minio_conn'
BUCKET = 'raw-data'


# Task: Data Quality Gate (Pandera)
def validate_flight_data(**kwargs):
    """
    Validate all new CSVs against the Pandera FlightSchema.
    - Valid files   -> moved to validated/
    - Invalid files -> moved to quarantine/
    - If ANY file fails -> raise AirflowFailException to block Spark
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix='flights_')
    if not keys:
        logger.warning("No new files to validate.")
        return

    logger.info(f"Found {len(keys)} file(s) to validate against FlightSchema")

    failed_files = []
    validated_files = []

    for key in keys:
        file_obj = s3_hook.get_key(key, BUCKET)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(file_content))

        try:
            # lazy=True collects ALL schema errors before raising
            FlightSchema.validate(df, lazy=True)
            logger.info(f"PASSED validation: {key} ({len(df)} records)")

            # Move to validated/
            new_key = f"validated/{key.split('/')[-1]}"
            s3_hook.copy_object(
                source_bucket_key=key, dest_bucket_key=new_key,
                source_bucket_name=BUCKET, dest_bucket_name=BUCKET
            )
            s3_hook.delete_objects(BUCKET, key)
            validated_files.append(key)

        except pandera.errors.SchemaErrors as e:
            error_count = len(e.failure_cases)
            logger.error(f"FAILED validation: {key} — {error_count} errors found")
            logger.error(f"Failure details:\n{e.failure_cases.to_string()}")

            # Quarantine bad data
            new_key = f"quarantine/{key.split('/')[-1]}"
            s3_hook.copy_object(
                source_bucket_key=key, dest_bucket_key=new_key,
                source_bucket_name=BUCKET, dest_bucket_name=BUCKET
            )
            s3_hook.delete_objects(BUCKET, key)
            failed_files.append(key)

    # DATA QUALITY GATE: Block downstream tasks if any file failed
    if failed_files:
        logger.critical(
            f"Data quality gate BLOCKED pipeline. "
            f"{len(failed_files)} file(s) quarantined: {failed_files}"
        )
        raise AirflowFailException(
            f"Data quality check FAILED. "
            f"{len(failed_files)} file(s) quarantined: {failed_files}. "
            f"Spark processing blocked."
        )

    logger.info(f"All {len(validated_files)} file(s) passed validation — pipeline may proceed.")


# Task: Automated Archive Cleanup
def archive_processed_files(**kwargs):
    """
    Move files from validated/ to archived/{date}/ after Spark processes them.
    Prevents files from being reprocessed on the next DAG run.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    today = datetime.now().strftime('%Y-%m-%d')

    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix='validated/')
    if not keys:
        logger.warning("No files to archive.")
        return

    archived_count = 0
    for key in keys:
        filename = key.split('/')[-1]
        if not filename:
            continue

        archive_key = f"archived/{today}/{filename}"
        s3_hook.copy_object(
            source_bucket_key=key, dest_bucket_key=archive_key,
            source_bucket_name=BUCKET, dest_bucket_name=BUCKET
        )
        s3_hook.delete_objects(BUCKET, key)
        archived_count += 1
        logger.info(f"Archived: {key} -> {archive_key}")

    logger.info(f"Archived {archived_count} file(s) to archived/{today}/")


# DAG Definition
with DAG(
    'flight_analytics_pipeline',
    default_args=default_args,
    description='Processing Engine: Validate -> Spark Transform -> Archive',
    schedule_interval='@hourly',
    catchup=False,
    tags=['analytics', 'processing', 'flight-data'],
) as dag:

    # 1. Wait for new flight data in MinIO
    wait_for_file = S3KeySensor(
        task_id='wait_for_flight_data',
        bucket_name=BUCKET,
        bucket_key='flights_*.csv',
        wildcard_match=True,
        aws_conn_id=MINIO_CONN_ID,
        timeout=18 * 60 * 60,
        poke_interval=30,
    )

    # 2. Data Quality Gate — Pandera validation
    validate_data = PythonOperator(
        task_id='validate_with_pandera',
        python_callable=validate_flight_data,
    )

    # 3. Spark Processing (Transform & Load to Postgres)
    process_data = SparkSubmitOperator(
        task_id='process_flight_data_spark',
        application='/opt/airflow/spark/jobs/process_flights.py',
        conn_id='spark_default',
        jars='/opt/airflow/spark/jars/postgresql-42.7.6.jar',
        packages='org.apache.hadoop:hadoop-aws:3.3.4',
        conf={
            'spark.driver.host': 'airflow-scheduler',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minio_admin',
            'spark.hadoop.fs.s3a.secret.key': 'minio_password_321',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        },
        verbose=True,
    )

    # 4. Archive processed files so they aren't reprocessed
    archive_files = PythonOperator(
        task_id='archive_processed_files',
        python_callable=archive_processed_files,
    )

    # Pipeline flow
    wait_for_file >> validate_data >> process_data >> archive_files
