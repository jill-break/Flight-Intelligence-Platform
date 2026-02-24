from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, when
import os

# Initialize Spark with S3 and Postgres support
spark = SparkSession.builder \
    .appName("FlightIntelligenceGold") \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. READ: Pull validated data from MinIO
df = spark.read.csv("s3a://raw-data/validated/*.csv", header=True, inferSchema=True)

# 2. TRANSFORM & SECURE: Demonstrating Data Engineering & Cybersecurity
# - Hash transaction IDs for privacy
# - Add a 'load_timestamp'
# - High-Priority Flag for flights with > 250 passengers
processed_df = df.withColumn("transaction_id", sha2(col("transaction_id"), 256)) \
                 .withColumn("load_timestamp", lit(os.popen('date').read().strip())) \
                 .withColumn("priority_flight", when(col("passenger_count") > 250, "HIGH").otherwise("NORMAL"))

# 3. WRITE: Load into the Analytics Warehouse (Postgres)
# Write to the dedicated analytics DB, not the Airflow metadata DB
db_url = "jdbc:postgresql://postgres_analytics:5432/retail_db"
db_properties = {
    "user": os.getenv("POSTGRES_USER", "warehouse_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "secure_password_789"),
    "driver": "org.postgresql.Driver"
}

processed_df.write.jdbc(url=db_url, table="flight_analytics.gold_flights", mode="overwrite", properties=db_properties)

print("Transformation and Load to Postgres complete.")