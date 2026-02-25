# Flight Intelligence Platform — User Guide

> **Complete reference** for running the project from scratch to teardown.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Environment Setup](#2-environment-setup)
3. [Build & Start Services](#3-build--start-services)
4. [Airflow Connection Setup](#4-airflow-connection-setup)
5. [Database Schema Setup](#5-database-schema-setup)
6. [Generate Flight Data](#6-generate-flight-data)
7. [Trigger & Monitor DAGs](#7-trigger--monitor-dags)
8. [Test Individual Tasks](#8-test-individual-tasks)
9. [Verify Data in Postgres](#9-verify-data-in-postgres)
10. [Connect Metabase to Postgres](#10-connect-metabase-to-postgres)
11. [Run Tests](#11-run-tests)
12. [Spark Connection Management](#12-spark-connection-management)
13. [Useful Docker Commands](#13-useful-docker-commands)
14. [Full Teardown](#14-full-teardown)

---

## 1. Prerequisites

| Tool | Version |
|------|---------|
| Docker Desktop | 4.x+ |
| Docker Compose | v2+ (bundled with Desktop) |
| Python | 3.10+ (for local tests) |
| Git | Any recent version |

Ensure Docker Desktop is **running** before proceeding.

---

## 2. Environment Setup

Create a `.env` file in the project root with these values:

```bash
# .env
POSTGRES_USER=warehouse_admin
POSTGRES_PASSWORD=secure_password_789
POSTGRES_DB=retail_db

MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=minio_password_321

AWS_ACCESS_KEY_ID=minio_admin
AWS_SECRET_ACCESS_KEY=minio_password_321
```

---

## 3. Build & Start Services

```bash
# Build the custom Airflow image and start all 9 services
docker compose up --build -d
```

**Verify all containers are running:**

```bash
docker compose ps
```

Expected containers:

| Container | Port | Purpose |
|-----------|------|---------|
| `postgres_airflow` | (internal) | Airflow metadata DB |
| `airflow_webserver` | `8080` | Airflow UI |
| `airflow-scheduler` | (internal) | DAG scheduler |
| `minio_storage` | `9000` / `9001` | S3-compatible storage |
| `spark_master` | `8081` / `7077` | Spark master |
| `spark_worker` | (internal) | Spark executor |
| `postgres_analytics` | `5433` | Analytics warehouse |
| `metabase_viz` | `3000` | BI dashboards |

**Web UIs:**

- Airflow: [http://localhost:8080](http://localhost:8080) (`admin` / `admin`)
- MinIO Console: [http://localhost:9001](http://localhost:9001) (`minio_admin` / `minio_password_321`)
- Spark Master: [http://localhost:8081](http://localhost:8081)
- Metabase: [http://localhost:3000](http://localhost:3000)

---

## 4. Airflow Connection Setup

### a) MinIO Connection (`minio_conn`)

```bash
docker exec airflow-scheduler airflow connections add minio_conn \
  --conn-type aws \
  --conn-extra '{"aws_access_key_id": "minio_admin", "aws_secret_access_key": "minio_password_321", "endpoint_url": "http://minio:9000"}'
```

### b) Spark Connection (`spark_default`)

```bash
docker exec airflow-scheduler airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077
```

### Verify Connections

```bash
docker exec airflow-scheduler airflow connections list
```

---

## 5. Database Schema Setup

Create the `flight_analytics` schema in the analytics Postgres database so Spark can write to it:

```bash
docker exec postgres_analytics psql -U warehouse_admin -d retail_db \
  -c "CREATE SCHEMA IF NOT EXISTS flight_analytics;"
```

**Verify:**

```bash
docker exec postgres_analytics psql -U warehouse_admin -d retail_db \
  -c "\dn"
```

---

## 6. Generate Flight Data

### a) Create the MinIO bucket (if not already created)

Open the MinIO Console at [http://localhost:9001](http://localhost:9001) and create a bucket named `raw-data`, **or** use the Airflow CLI:

```bash
docker exec airflow-scheduler python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id='minio_conn')
hook.get_conn().create_bucket(Bucket='raw-data')
print('Bucket created.')
"
```

### b) Generate CSV files locally

```bash
# Single batch (50 records)
python scripts/flight_generator.py 50

# Continuous mode (generates a file every 30s, alternating clean/dirty)
python scripts/flight_generator.py 50 30
```

CSV files are written to the `data/` directory.

---

## 7. Trigger & Monitor DAGs

### a) Unpause the DAGs

```bash
docker exec airflow-scheduler airflow dags unpause raw_ingestion_pipeline
docker exec airflow-scheduler airflow dags unpause flight_analytics_pipeline
```

### b) Trigger manually

```bash
# Trigger raw ingestion (picks up files from data/ → MinIO)
docker exec airflow-scheduler airflow dags trigger raw_ingestion_pipeline

# Trigger analytics pipeline (MinIO → Pandera → Spark → Postgres)
docker exec airflow-scheduler airflow dags trigger flight_analytics_pipeline
```

### c) Monitor in UI

Open [http://localhost:8080](http://localhost:8080) → DAGs → Click on the DAG name → Graph view to see task progress.

---

## 8. Test Individual Tasks

Use `airflow tasks test` to run individual tasks without recording state:

### Raw Ingestion DAG

```bash
# Test file sensor
docker exec airflow-scheduler airflow tasks test raw_ingestion_pipeline wait_for_local_file 2026-02-23

# Test upload to MinIO
docker exec airflow-scheduler airflow tasks test raw_ingestion_pipeline upload_to_minio 2026-02-23
```

### Analytics Pipeline DAG

```bash
# Test S3 file sensor
docker exec airflow-scheduler airflow tasks test flight_analytics_pipeline wait_for_minio_files 2026-02-23

# Test Pandera validation
docker exec airflow-scheduler airflow tasks test flight_analytics_pipeline validate_with_pandera 2026-02-23

# Test Spark job
docker exec airflow-scheduler airflow tasks test flight_analytics_pipeline process_flight_data_spark 2026-02-23

# Test archiving
docker exec airflow-scheduler airflow tasks test flight_analytics_pipeline archive_processed_files 2026-02-23
```

---

## 9. Verify Data in Postgres

### a) Check the table exists

```bash
docker exec postgres_analytics psql -U warehouse_admin -d retail_db \
  -c "\dt flight_analytics.*"
```

### b) Preview the data

```bash
docker exec postgres_analytics psql -U warehouse_admin -d retail_db \
  -c "SELECT * FROM flight_analytics.gold_flights LIMIT 10;"
```

### c) Count total records

```bash
docker exec postgres_analytics psql -U warehouse_admin -d retail_db \
  -c "SELECT COUNT(*) FROM flight_analytics.gold_flights;"
```

### d) Check column names

```bash
docker exec postgres_analytics psql -U warehouse_admin -d retail_db \
  -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='flight_analytics' AND table_name='gold_flights';"
```

---

## 10. Connect Metabase to Postgres

1. Open **Metabase** at [http://localhost:3000](http://localhost:3000)
2. Complete initial setup (create admin account)
3. Go to ⚙️ **Admin** → **Databases** → **Add database**
4. Fill in:

| Field | Value |
|-------|-------|
| Database type | PostgreSQL |
| Display name | Flight Analytics Warehouse |
| Host | `postgres_analytics` |
| Port | `5432` |
| Database name | `retail_db` |
| Username | `warehouse_admin` |
| Password | `secure_password_789` |

5. Click **Save**
6. Optional: Under **Show advanced options** → **Schemas**, select only `flight_analytics`

---

## 11. Run Tests

### a) Install test dependencies (local)

```bash
pip install pytest pandera pandas faker
```

### b) Run all tests

```bash
pytest tests/ -v
```

### c) Run individual test files

```bash
# Test flight data generator
pytest tests/test_flight_generator.py -v

# Test Pandera schema
pytest tests/test_flight_schema.py -v

# Test DAG integrity (imports & structure)
pytest tests/test_dag_integrity.py -v
```

---

## 12. Spark Connection Management

### List connections

```bash
docker exec airflow-scheduler airflow connections list
```

### Delete Spark connection

```bash
docker exec airflow-scheduler airflow connections delete spark_default
```

### Re-create Spark connection

```bash
docker exec airflow-scheduler airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077
```

### Verify Spark is reachable

```bash
docker exec airflow-scheduler airflow connections get spark_default
```

### View Spark Master UI

Open [http://localhost:8081](http://localhost:8081) to see workers, running applications, and completed jobs.

---

## 13. Useful Docker Commands

```bash
# View logs for a specific container
docker logs airflow-scheduler --tail 100
docker logs spark_master --tail 50
docker logs postgres_analytics --tail 50

# Exec into a container for debugging
docker exec -it airflow-scheduler bash
docker exec -it postgres_analytics psql -U warehouse_admin -d retail_db

# Restart a single service
docker compose restart airflow-scheduler
docker compose restart spark-master spark-worker

# Check container resource usage
docker stats --no-stream
```

---

## 14. Full Teardown

### a) Stop all services (keep data volumes)

```bash
docker compose down
```

### b) Stop and remove everything (volumes, networks, images)

```bash
# Remove containers + volumes (deletes all data!)
docker compose down -v

# Also remove built images
docker compose down -v --rmi all
```

### c) Clean up dangling resources

```bash
docker system prune -f
docker volume prune -f
```

### d) Delete Airflow connections before teardown (optional)

```bash
docker exec airflow-scheduler airflow connections delete spark_default
docker exec airflow-scheduler airflow connections delete minio_conn
```

---

## Quick Reference — Full Run Cheat Sheet

```bash
# 1. Start everything
docker compose up --build -d

# 2. Setup connections
docker exec airflow-scheduler airflow connections add minio_conn --conn-type aws --conn-extra '{"aws_access_key_id": "minio_admin", "aws_secret_access_key": "minio_password_321", "endpoint_url": "http://minio:9000"}'
docker exec airflow-scheduler airflow connections add spark_default --conn-type spark --conn-host "spark://spark-master" --conn-port 7077

# 3. Create schema
docker exec postgres_analytics psql -U warehouse_admin -d retail_db -c "CREATE SCHEMA IF NOT EXISTS flight_analytics;"

# 4. Generate data
python scripts/flight_generator.py 50

# 5. Unpause & trigger
docker exec airflow-scheduler airflow dags unpause raw_ingestion_pipeline
docker exec airflow-scheduler airflow dags unpause flight_analytics_pipeline
docker exec airflow-scheduler airflow dags trigger raw_ingestion_pipeline
docker exec airflow-scheduler airflow dags trigger flight_analytics_pipeline

# 6. Verify
docker exec postgres_analytics psql -U warehouse_admin -d retail_db -c "SELECT COUNT(*) FROM flight_analytics.gold_flights;"

# 7. Teardown
docker compose down -v
```
