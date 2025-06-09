from confluent_kafka import Producer
import requests
import json


def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print(
            "Produced event to topic {topic}: key = {key:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8')))


def solution():
    year = 2023
    month = 10
    config = {'bootstrap.servers': 'localhost:9092', 'acks': 'all'}
    producer = Producer(config)

    topic = 'stock_transactions'
    url = 'http://127.0.0.1:5000/api/get_data'
    params = {'year': year, 'month': month, 'day': '01', 'offset': 0, 'limit': 100}

    while True:
        try:
            r = requests.get(url=url, params=params)
            data = r.json()

            if data['status'] == 'error':
                break

            if data['status'] == 'success' or data['status'] == 'complete':
                key = str(year) + '_' + str(month) + '_' + str(params['offset'])
                print(key)
                value = json.dumps(data['data'])
                producer.produce(topic, value, key, callback=delivery_callback)

                if data['status'] == 'complete':
                    break

                params['offset'] += 100
        except Exception as e:
            print(e)
            break
    producer.flush()
solution()