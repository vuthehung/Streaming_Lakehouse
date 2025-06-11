from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hungde',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

with DAG(
    'test',
     default_args=default_args,
     schedule_interval='@daily',
     catchup=False
) as dag:
    test_task = BashOperator(
        task_id="run_a_simple_test",
        bash_command='echo "Hello Airflow! Your test DAG is working perfectly."',
    )