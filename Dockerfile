FROM apache/airflow:2.8.1-python3.11

USER root
COPY requirements.txt /requirements.txt

USER airflow

# Use Airflow's official constraints to avoid dependency conflicts
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"