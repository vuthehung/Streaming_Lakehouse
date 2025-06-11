from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'hungde',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

SPARK_JOB_FILE = "test.py"
SPARK_JOB_PATH = f"/opt/airflow/code/{SPARK_JOB_FILE}"


SPARK_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.4.2"
]

with DAG(
    'test',
     default_args=default_args,
     schedule_interval='@daily',
     catchup=False
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_job_task",
        application=SPARK_JOB_PATH,
        conn_id="spark_conn",
        verbose=True,
        packages=",".join(SPARK_PACKAGES)
    )