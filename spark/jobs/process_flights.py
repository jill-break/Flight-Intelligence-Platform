from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, when
import os
import logging

# ──────────────────────────────────────────────
# Logger Setup
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('process_flights')

# Initialize Spark with S3 and Postgres support
logger.info("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("FlightIntelligenceGold") \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
logger.info("Spark session initialized successfully.")

# 1. READ: Pull validated data from MinIO
logger.info("Reading validated flight data from MinIO (s3a://raw-data/validated/*.csv)...")
df = spark.read.csv("s3a://raw-data/validated/*.csv", header=True, inferSchema=True)
record_count = df.count()
logger.info(f"Loaded {record_count} records from MinIO.")

# 2. TRANSFORM & SECURE: Demonstrating Data Engineering & Cybersecurity
# - Hash transaction IDs for privacy
# - Add a 'load_timestamp'
# - High-Priority Flag for flights with > 250 passengers
logger.info("Applying transformations: SHA-256 hashing, timestamps, priority classification...")
processed_df = df.withColumn("transaction_id", sha2(col("transaction_id"), 256)) \
                 .withColumn("load_timestamp", lit(os.popen('date').read().strip())) \
                 .withColumn("priority_flight", when(col("passenger_count") > 250, "HIGH").otherwise("NORMAL"))

priority_count = processed_df.filter(col("priority_flight") == "HIGH").count()
logger.info(f"Transformation complete: {priority_count} HIGH priority flights identified.")

# 3. WRITE: Load into the Analytics Warehouse (Postgres)
# Write to the dedicated analytics DB, not the Airflow metadata DB
db_url = "jdbc:postgresql://postgres_analytics:5432/retail_db"
db_properties = {
    "user": os.getenv("POSTGRES_USER", "warehouse_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "secure_password_789"),
    "driver": "org.postgresql.Driver"
}

logger.info(f"Writing {record_count} records to PostgreSQL (flight_analytics.gold_flights)...")
processed_df.write.jdbc(url=db_url, table="flight_analytics.gold_flights", mode="append", properties=db_properties)

logger.info("Transformation and Load to Postgres complete.")