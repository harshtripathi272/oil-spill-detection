FROM apache/airflow:2.8.0

ARG AIRFLOW_VERSION=2.8.0
ARG PYTHON_VERSION=3.8

USER airflow

COPY requirements-docker.txt /tmp/requirements.txt

# Install minimal project dependencies for Airflow + ASF search
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements.txt
