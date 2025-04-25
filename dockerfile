FROM apache/airflow:2.9.2

USER root

# Install default JDK and required system packages
RUN apt-get update \
    && apt-get install -y \
        default-jdk \
        python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages as the airflow user
RUN pip install --use-deprecated=legacy-resolver \
        apache-airflow-providers-ssh==3.12.0  \
        apache-airflow-providers-apache-spark \
        kafka-python \
        requests \
        dbt-snowflake \
        dbt-core \
        hdfs

# Ensure the Airflow DB directory exists and has correct ownership/permissions
RUN mkdir -p /opt/airflow/db \
 && chown -R 50000:0 /opt/airflow/db \
 && chmod -R g+rw /opt/airflow/db
