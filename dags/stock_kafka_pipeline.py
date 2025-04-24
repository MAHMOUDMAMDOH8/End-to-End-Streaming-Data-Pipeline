from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import socket


# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='stock_kafka_pipeline',
    default_args=default_args,
    description='Run producer and load-to-bronze tasks every 10 minutes',
    schedule_interval='*/10 * * * *',
    catchup=False,
) as dag:


    produce_task = BashOperator(
        task_id='Kafka_Producer',
        bash_command='python3 /opt/airflow/Scripts/alpha_vantage_producer.py',
    )

    load_task = BashOperator(
        task_id='Kafka_Consumer',
        bash_command='python3 /opt/airflow/Scripts/load_to_bronze.py',
    )

    upload_task = BashOperator(
        task_id="upload_to_hdfs",
        bash_command='python3 /opt/airflow/Scripts/upload_to_hdfs.py',
    )
    run_spark_test = SparkSubmitOperator(
        task_id='run_spark_test',
        application='/opt/spark-apps/test.py',
        conn_id='spark_default',
        verbose=True,
    )

    produce_task >> load_task >> upload_task >> run_spark_test
 