from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from confluent_kafka import Producer
import requests
import json
import time

default_args = {
    'owner': 'hungde',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'retries': 3,
    'retry_delay': 300
}

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(
            f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')}"
        )
def stream_data_to_kafka():
    current_year = 2025
    current_month = 6
    current_day = 7

    config = {'bootstrap.servers': 'broker:29092', 'acks': 'all'}
    producer = Producer(config)
    topic = f'stock_transactions_{current_year}_{current_month}_{current_day}'
    url = 'http://flask:5000/api/get_data'
    params = {'year': current_year, 'month': current_month, 'day': current_day, 'offset': 0, 'limit': 100}

    print("Bắt đầu quá trình stream dữ liệu...")

    while True:
        try:
            print(f"Đang lấy dữ liệu từ API với offset: {params['offset']}")
            r = requests.get(url=url, params=params)
            r.raise_for_status()
            data = r.json()

            if data.get('status') == 'error':
                print(f"API trả về lỗi: {data.get('message', 'Không có thông điệp lỗi')}. Dừng lại.")
                break

            if data.get('data'):
                key = f"{current_year}_{current_month}_{current_day}_{params['offset']}"
                value = json.dumps(data['data'])
                producer.produce(topic, value, key, callback=delivery_callback)
                producer.poll(0)
            else:
                print("API không trả về dữ liệu.")

            if data.get('status') == 'complete':
                print("API báo hiệu đã hoàn thành. Dừng lại.")
                break

            params['offset'] += 100

        except requests.exceptions.RequestException as e:
            print(f"Lỗi kết nối đến API: {e}. Dừng lại.")
            break
        except Exception as e:
            print(f"Một lỗi không xác định đã xảy ra: {e}. Dừng lại.")
            break
        # print("Hoàn thành 1 chu kỳ.")
        # time.sleep(10)
    producer.flush()


with DAG(
    'stream_dag',
     default_args=default_args,
     schedule_interval='@daily',
     catchup=False
) as dag:
    stream_task = PythonOperator(
        task_id="stream_from_api_to_kafka_task",
        python_callable=stream_data_to_kafka,
    )
    submit_streaming_job = SparkSubmitOperator(
        task_id="submit_kafka_to_iceberg_job",
        application="/opt/airflow/code/stream_kafka_iceberg.py",
        conn_id="spark_conn",
        verbose=True,
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    )

    [stream_task, submit_streaming_job]
