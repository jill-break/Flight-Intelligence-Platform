"""
Flight Analytics Pipeline DAG — Processing Engine
====================================================
Waits for files in MinIO -> Validates with Pandera -> Transforms with Spark -> Archives

Quarantine Lifecycle (whole-file):
  - If ANY row in a file fails Pandera validation, the ENTIRE file is quarantined
  - A cleaning branch applies fixes (clamp values, strip whitespace, etc.)
  - Recovered rows are re-validated and moved to validated/ for Spark processing
  - Unfixable rows are tracked in dropped_rows/ with filename + transaction_id + reason

Task flow:
  wait_for_file -> validate_data -> [branch]
                                      |-> (clean files) -> process_data -> archive_files
                                      |-> (dirty files) -> clean_quarantined_data
                                                              |-> track_dropped_rows
                                                              |-> reingest_cleaned_data -> process_data
"""

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import pandera
import io
import json
import logging

from spark.schemas.flight_schema import FlightSchema
from quarantine_cleaner import clean_dataframe, build_dropped_rows_report

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


# ──────────────────────────────────────────────
# Task: Data Quality Gate (Pandera) + Branch
# ──────────────────────────────────────────────
def validate_flight_data(**kwargs):
    """
    Validate all new CSVs against the Pandera FlightSchema.
    - Valid files   -> moved to validated/
    - Invalid files -> moved to quarantine/ (whole file)
    Returns the branch to follow based on whether files were quarantined.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    ti = kwargs['ti']

    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix='flights_')
    if not keys:
        logger.warning("No new files to validate.")
        return 'process_flight_data_spark'

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

            # Move to validated/clean/
            new_key = f"validated/clean/{key.split('/')[-1]}"
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

            # Quarantine the ENTIRE file
            new_key = f"quarantine/{key.split('/')[-1]}"
            s3_hook.copy_object(
                source_bucket_key=key, dest_bucket_key=new_key,
                source_bucket_name=BUCKET, dest_bucket_name=BUCKET
            )
            s3_hook.delete_objects(BUCKET, key)
            failed_files.append(key)

    # Push quarantine info for downstream tasks
    ti.xcom_push(key='quarantined_files', value=failed_files)
    ti.xcom_push(key='validated_files', value=validated_files)

    if failed_files:
        logger.warning(
            f"Data quality gate: {len(failed_files)} file(s) quarantined, "
            f"{len(validated_files)} file(s) validated."
        )
        # Branch to both: process valid files AND clean quarantined files
        branches = []
        if validated_files:
            branches.append('process_clean_data_spark')
        branches.append('clean_quarantined_data')
        return branches

    logger.info(f"All {len(validated_files)} file(s) passed validation — pipeline may proceed.")
    return 'process_clean_data_spark'


# ──────────────────────────────────────────────
# Task: Clean Quarantined Data
# ──────────────────────────────────────────────
def clean_quarantined_data(**kwargs):
    """
    Read quarantined files from MinIO, apply cleaning rules,
    re-validate, and push results for downstream tasks.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    ti = kwargs['ti']

    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix='quarantine/')
    if not keys:
        logger.warning("No quarantined files to clean.")
        return

    logger.info(f"Cleaning {len(keys)} quarantined file(s)")

    all_cleaned = []
    all_dropped = []
    cleaned_filenames = []

    for key in keys:
        filename = key.split('/')[-1]
        if not filename:
            continue

        file_obj = s3_hook.get_key(key, BUCKET)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(file_content))

        logger.info(f"Processing quarantined file: {filename} ({len(df)} rows)")

        # Apply cleaning pipeline
        cleaned_df, dropped_df = clean_dataframe(df, filename)

        if not cleaned_df.empty:
            all_cleaned.append(cleaned_df)
            cleaned_filenames.append(filename)

            # Write cleaned data to validated/recovered/ for Spark to pick up
            csv_buffer = cleaned_df.to_csv(index=False)
            cleaned_key = f"validated/recovered/cleaned_{filename}"
            s3_hook.load_string(
                string_data=csv_buffer,
                key=cleaned_key,
                bucket_name=BUCKET,
                replace=True,
            )
            logger.info(
                f"Recovered {len(cleaned_df)} rows from {filename} -> {cleaned_key}"
            )

        if not dropped_df.empty:
            all_dropped.append(dropped_df)

        # Remove from quarantine after processing
        s3_hook.delete_objects(BUCKET, key)
        logger.info(f"Removed processed quarantine file: {key}")

    # Push dropped data for the tracking task
    if all_dropped:
        combined_dropped = pd.concat(all_dropped, ignore_index=True)
        ti.xcom_push(
            key='dropped_rows_json',
            value=combined_dropped.to_json(orient='records'),
        )
        logger.info(f"Total dropped rows across all files: {len(combined_dropped)}")
    else:
        ti.xcom_push(key='dropped_rows_json', value='[]')

    total_recovered = sum(len(df) for df in all_cleaned)
    logger.info(f"Quarantine cleaning complete: {total_recovered} rows recovered")


# ──────────────────────────────────────────────
# Task: Track Dropped Rows
# ──────────────────────────────────────────────
def track_dropped_rows(**kwargs):
    """
    Write dropped rows to the dropped-rows bucket in MinIO.
    Each file is saved as: dropped_rows/{date}/{source_filename}
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    ti = kwargs['ti']

    dropped_json = ti.xcom_pull(
        task_ids='clean_quarantined_data', key='dropped_rows_json'
    )

    if not dropped_json or dropped_json == '[]':
        logger.info("No dropped rows to track.")
        return

    dropped_df = pd.read_json(io.StringIO(dropped_json), orient='records')
    report = build_dropped_rows_report(dropped_df, 'combined')

    today = datetime.now().strftime('%Y-%m-%d')

    # Group by source filename and write separate reports
    for source_file, group in report.groupby('source_filename'):
        report_key = f"dropped_rows/{today}/{source_file}"
        csv_data = group.to_csv(index=False)

        s3_hook.load_string(
            string_data=csv_data,
            key=report_key,
            bucket_name='dropped-rows',
            replace=True,
        )
        logger.info(
            f"Tracked {len(group)} dropped rows -> dropped-rows/{report_key}"
        )

    logger.info(f"Dropped row tracking complete: {len(report)} total rows tracked")


# ──────────────────────────────────────────────
# Task: Automated Archive Cleanup
# ──────────────────────────────────────────────
def archive_processed_files(**kwargs):
    """
    Move files from a validated prefix to archived/{date}/ after Spark processes them.
    Prevents files from being reprocessed on the next DAG run.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    today = datetime.now().strftime('%Y-%m-%d')
    
    prefix = kwargs.get('prefix', 'validated/')

    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix)
    if not keys:
        logger.warning(f"No files to archive in {prefix}.")
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


# ══════════════════════════════════════════════
# DAG Definition
# ══════════════════════════════════════════════
with DAG(
    'flight_analytics_pipeline',
    default_args=default_args,
    description='Processing Engine: Validate -> Clean Quarantine -> Spark Transform -> Archive',
    schedule_interval='@hourly',
    catchup=False,
    tags=['analytics', 'processing', 'flight-data', 'quarantine'],
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

    # 2. Data Quality Gate — Pandera validation + branch
    validate_data = BranchPythonOperator(
        task_id='validate_with_pandera',
        python_callable=validate_flight_data,
    )

    # 3. Spark Processing for Clean Data
    process_clean_data = SparkSubmitOperator(
        task_id='process_clean_data_spark',
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
        application_args=['s3a://raw-data/validated/clean/*.csv'],
        verbose=True,
    )

    # 4. Archive processed clean files
    archive_clean = PythonOperator(
        task_id='archive_clean_files',
        python_callable=archive_processed_files,
        op_kwargs={'prefix': 'validated/clean/'},
    )

    # 5. Quarantine cleaning branch
    clean_quarantine = PythonOperator(
        task_id='clean_quarantined_data',
        python_callable=clean_quarantined_data,
    )

    # 6. Track dropped rows
    track_drops = PythonOperator(
        task_id='track_dropped_rows',
        python_callable=track_dropped_rows,
    )

    # 7. Spark Processing for Recovered Data
    process_recovered_data = SparkSubmitOperator(
        task_id='process_recovered_data_spark',
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
        application_args=['s3a://raw-data/validated/recovered/*.csv'],
        verbose=True,
    )

    # 8. Archive processed recovered files
    archive_recovered = PythonOperator(
        task_id='archive_recovered_files',
        python_callable=archive_processed_files,
        op_kwargs={'prefix': 'validated/recovered/'},
    )

    # ── Pipeline Flow ──────────────────────────────
    # Main path: wait -> validate -> [branch]
    wait_for_file >> validate_data

    # Clean path: validate -> Spark (clean) -> archive (clean)
    validate_data >> process_clean_data >> archive_clean

    # Quarantine path: validate -> clean -> [track, Spark (recovered) -> archive (recovered)]
    validate_data >> clean_quarantine
    clean_quarantine >> track_drops
    clean_quarantine >> process_recovered_data >> archive_recovered
