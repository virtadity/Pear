FROM apache/airflow:slim-3.4.0-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    clickhouse-connect \
    -r /requirements.txt \
