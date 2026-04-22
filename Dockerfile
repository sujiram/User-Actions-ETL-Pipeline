FROM apache/airflow:2.9.3-python3.11

USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow
WORKDIR /opt/airflow

COPY user_actions_etl_dag.py /opt/airflow/dags/user_actions_etl_dag.py
COPY ETL_Pipeline.py /opt/airflow/ETL_Pipeline.py
COPY Config.py /opt/airflow/Config.py
COPY raw_logs.json /opt/airflow/data/raw_logs.json
COPY schema.sql /opt/airflow/schema.sql

ENV PYTHONPATH=/opt/airflow