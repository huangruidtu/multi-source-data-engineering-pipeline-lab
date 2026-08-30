FROM apache/airflow:2.10.5-python3.12

USER root
RUN pip install --no-cache-dir "boto3>=1.34,<2.0" "psycopg[binary]>=3.1,<4.0" "pyarrow>=16"
USER airflow
