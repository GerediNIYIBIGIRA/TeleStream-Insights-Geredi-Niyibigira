FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-redis \
    psycopg2-binary
