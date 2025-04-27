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
        bash_command='python3 /opt/airflow/Scripts/Kafka/Producer.py',
    )

    Consume_task = BashOperator(
        task_id='Kafka_Consumer',
        bash_command='python3 /opt/airflow/Scripts/Kafka/Consumer.py',
    )

    upload_to_hdfs_task = BashOperator(
        task_id='Upload_to_HDFS',
        bash_command='python3 /opt/airflow/Scripts/hdfs/upload_to_hdfs.py',
    )

    cleaning_job_task = SparkSubmitOperator(
        task_id='Spark_Cleaning_Job',
        application='/opt/airflow/Scripts/Spark/bronze_to_silver_cleaning_job.py',
        conn_id='spark_default',
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='2g',
        num_executors=1,
        driver_memory='2g',
        verbose=False,
        conf={
            "spark.pyspark.python": "/usr/local/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "/usr/local/bin/python",
        },
    )

    upload_to_snowflake_task = BashOperator(
        task_id='Upload_to_Snowflake',
        bash_command='rm -r /opt/airflow/includes/local_warehose/silver/stock-prices && python3 /opt/airflow/Scripts/python/upload_to_snowflake.py',
    )


    produce_task >> Consume_task >> upload_to_hdfs_task >> cleaning_job_task >> upload_to_snowflake_task
 