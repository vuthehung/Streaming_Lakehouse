import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Thiết lập tham số
start_time = datetime(2023, 10, 1, 9, 0, 0)  # 9:00 AM
end_time = datetime(2023, 10, 1, 16, 0, 0)   # 4:00 PM
time_interval = timedelta(minutes=1)
stock_symbol = 'AAPL'
initial_price = 150.0
price_volatility = 0.5  # Biến động giá mỗi phút
transactions_per_minute = (0, 10)  # Số giao dịch mỗi phút
quantity_range = (1, 100)  # Số lượng cổ phiếu mỗi giao dịch

# Tạo danh sách thời gian
times = []
current_time = start_time
while current_time < end_time:
    times.append(current_time)
    current_time += time_interval

# Mô phỏng giá cổ phiếu (random walk)
prices = [initial_price]
for _ in range(1, len(times)):
    price_change = np.random.uniform(-price_volatility, price_volatility)
    new_price = prices[-1] + price_change
    prices.append(max(0, new_price))  # Giá không âm

# Tạo dữ liệu giao dịch
data = []
for t, p in zip(times, prices):
    num_transactions = np.random.randint(transactions_per_minute[0], transactions_per_minute[1] + 1)
    for _ in range(num_transactions):
        quantity = np.random.randint(quantity_range[0], quantity_range[1] + 1)
        order_type = np.random.choice(['buy', 'sell'])
        exchange = np.random.choice(['NASDAQ', 'NYSE'])
        transaction_id = f'tx_{np.random.randint(1000000, 9999999)}'
        record = {
            'transaction_id': transaction_id,
            'timestamp': t.strftime('%Y-%m-%d %H:%M:%S'),
            'stock_symbol': stock_symbol,
            'price': round(p, 2),
            'quantity': quantity,
            'order_type': order_type,
            'exchange': exchange
        }
        data.append(record)

# Tạo DataFrame
df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Tính volume cho mỗi phút
df['minute'] = df['timestamp'].dt.floor('min')  # Thay 'T' bằng 'min'
volume_per_minute = df.groupby('minute')['quantity'].sum().reset_index()
volume_per_minute.columns = ['timestamp', 'volume']

# Thêm volume vào dữ liệu
df = df.merge(volume_per_minute, on='timestamp', how='left')

# Lưu dữ liệu vào CSV
df.to_csv('stock_transactions_2023_10_01.csv', index=False)
print("Đã tạo file stock_transactions_2023_10_01.csv")