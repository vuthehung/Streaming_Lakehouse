from confluent_kafka import Producer
import requests
import json
import time
from datetime import datetime

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(
            f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')}"
        )
def stream_data_to_kafka():
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day

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
        print("Hoàn thành 1 chu kỳ.")
        time.sleep(10)
    producer.flush()

if __name__ == '__main__':
    stream_data_to_kafka()