FROM apache/airflow:2.9.2

USER airflow

RUN python3 -m pip install --upgrade pip \
&& pip install --use-deprecated=legacy-resolver \
     apache-airflow-providers-ssh==3.12.0  \
     apache-airflow-providers-apache-spark \
     kafka-python \
     requests \
     dbt-snowflake==1.9.2 \
     dbt-core==1.9.4 \
     hdfs

USER root


# Ensure the Airflow DB directory exists and has correct ownership/permissions
RUN mkdir -p /opt/airflow/db \
 && chown -R 50000:0 /opt/airflow/db \
 && chmod -R g+rw /opt/airflow/db

USER airflow