from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import io
import os
import sys
# Import your schema (ensure the path is in your PYTHONPATH)
from spark.schemas.flight_schema import FlightSchema
# For SparkSubmitOperator, if needed in future steps
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'senior_de',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def validate_flight_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    bucket = 'raw-data'
    
    # List files in the raw bucket
    keys = s3_hook.list_keys(bucket_name=bucket, prefix='flights_')
    if not keys:
        print("No new files to validate.")
        return

    for key in keys:
        # Load CSV from S3 into Memory
        file_obj = s3_hook.get_key(key, bucket)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(file_content))
        
        try:
            # PANDERA VALIDATION
            FlightSchema.validate(df)
            print(f"Validation successful for {key}")
            
            # Move to validated prefix
            new_key = f"validated/{key.split('/')[-1]}"
            s3_hook.copy_object(source_bucket_key=key, dest_bucket_key=new_key, 
                               source_bucket_name=bucket, dest_bucket_name=bucket)
            s3_hook.delete_objects(bucket, key)
            
        except Exception as e:
            print(f"Validation failed for {key}: {str(e)}")
            # Move to quarantine
            new_key = f"quarantine/{key.split('/')[-1]}"
            s3_hook.copy_object(source_bucket_key=key, dest_bucket_key=new_key, 
                               source_bucket_name=bucket, dest_bucket_name=bucket)
            s3_hook.delete_objects(bucket, key)

with DAG('flight_data_ingestion', 
         default_args=default_args, 
         schedule_interval='@hourly', 
         catchup=False) as dag:

    # 1. Wait for file
    wait_for_file = S3KeySensor(
        task_id='wait_for_flight_data',
        bucket_name='raw-data',
        bucket_key='flights_*.csv',
        wildcard_match=True,
        aws_conn_id='minio_conn',
        timeout=18 * 60 * 60,
        poke_interval=30
    )

    # 2. Validate with Pandera
    validate_data = PythonOperator(
        task_id='validate_with_pandera',
        python_callable=validate_flight_data
    )

    # 3. Spark Processing Task
    process_validated_data = SparkSubmitOperator(
        task_id='process_flight_data_spark',
        application='/opt/airflow/spark/jobs/process_flights.py',
        conn_id='spark_default',
        # Point directly to the JAR in the mounted volume
        jars='/opt/airflow/spark/jars/postgresql-42.7.2.jar', 
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio_storage:9000",
            "spark.hadoop.fs.s3a.access.key": "minio_admin",
            "spark.hadoop.fs.s3a.secret.key": "minio_password_321",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        verbose=True
    )

    wait_for_file >> validate_data >> process_validated_data