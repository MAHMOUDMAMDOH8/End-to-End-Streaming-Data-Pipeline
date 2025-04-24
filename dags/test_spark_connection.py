from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_spark_connection',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_spark_ping = BashOperator(
        task_id='ping_spark',
        bash_command='curl -s http://spark-container:8080 || echo "Spark not reachable"',
    )

    run_spark_job = SparkSubmitOperator(
        task_id='run_simple_spark_job',
        application='./Scripts/test.py',  # Replace with the actual path to your Spark job script
        conn_id='spark_default',  # Ensure this connection is configured in Airflow
        application_args=['arg1', 'arg2'],  # Replace with any arguments your Spark job needs
        verbose=True,
    )

    test_spark_ping >> run_spark_job
