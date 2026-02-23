FROM apache/airflow:2.8.1-python3.11

# Copy requirements file from host to container
COPY requirements.txt /opt/airflow/requirements.txt

# Install dependencies as the airflow user
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt