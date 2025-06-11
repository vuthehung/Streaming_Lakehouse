from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
import requests
import json

default_args = {
    'owner': 'hungde',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(
            f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')}"
        )
def stream_data_to_kafka():
    year = 2025
    month = 6
    day = 7

    config = {'bootstrap.servers': 'broker:29092', 'acks': 'all'}
    producer = Producer(config)
    topic = f'stock_transactions_{year}_{month}_{day}'
    url = 'http://flask:5000/api/get_data'
    params = {'year': year, 'month': month, 'day': day, 'offset': 0, 'limit': 100}

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
                key = f"{year}_{month}_{day}_{params['offset']}"
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

    print("Quá trình stream kết thúc. Đang đợi gửi hết message còn lại...")
    producer.flush()
    print("Hoàn thành.")
with DAG(
    'test_kafka',
     default_args=default_args,
     schedule_interval='@daily',
     catchup=False
) as dag:
    stream_task = PythonOperator(
        task_id="stream_from_api_to_kafka_task",
        python_callable=stream_data_to_kafka,
    )