from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'hungde',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


with DAG(
    'spark_streaming_to_iceberg',
     default_args=default_args,
     schedule_interval='@daily',
     catchup=False
) as dag:
    submit_streaming_job = SparkSubmitOperator(
        task_id="submit_kafka_to_iceberg_job",
        application="/opt/airflow/code/main.py",
        conn_id="spark_conn",
        verbose=True,
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    )