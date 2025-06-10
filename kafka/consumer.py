# consumer.py (cập nhật để xử lý từng giao dịch)

from confluent_kafka import Consumer
import json


def solution_consumer():
    """
    Hàm này khởi tạo một Kafka Consumer, lắng nghe một topic cụ thể,
    và xử lý từng giao dịch trong mỗi message nhận được.
    """

    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'stock-transaction-consumers',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    topic = 'stock_transactions'
    consumer.subscribe([topic])
    print(f"Consumer đang lắng nghe topic '{topic}'...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("Lỗi Consumer: {}".format(msg.error()))
                continue

            # --- 5a. Xử lý message tổng thể ---
            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8')

            # Chuyển đổi chuỗi JSON (là một mảng) thành list các dictionary
            transactions_list = json.loads(value)

            print("=" * 60)
            print(f"Nhận được message từ Key: {key}")
            print(f"Số lượng giao dịch trong message này: {len(transactions_list)}")
            print("--- Bắt đầu xử lý từng giao dịch ---")

            # --- 5b. Xử lý từng giao dịch trong message ---
            # Lặp qua từng dictionary (từng giao dịch) trong list
            for transaction in transactions_list:
                # Giờ đây bạn có thể truy cập từng trường dữ liệu của giao dịch
                tx_id = transaction.get('transaction_id', 'N/A')
                symbol = transaction.get('stock_symbol', 'N/A')
                order_type = transaction.get('order_type', 'N/A')
                price = transaction.get('price', 0)
                quantity = transaction.get('quantity', 0)

                # In ra thông tin chi tiết của mỗi giao dịch
                print(
                    f"  -> ID: {tx_id}, Mã CK: {symbol}, Lệnh: {order_type.upper()}, "
                    f"Giá: ${price:.2f}, Số lượng: {quantity}"
                )

            print("=" * 60)
            print("\n")

    except KeyboardInterrupt:
        print("Đã dừng consumer.")
    finally:
        consumer.close()


if __name__ == '__main__':
    solution_consumer()