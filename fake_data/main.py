import pandas as pd
from datetime import datetime, timedelta
import random

# current_date = datetime.now()
current_year = 2025
current_month = 6
current_day = 20

start_date = datetime(current_year, current_month, current_day, 9, 30)
end_date = datetime(current_year, current_month, current_day, 16, 0)
trading_seconds = int((end_date - start_date).total_seconds())

# Define stock information with exchange and initial base price
stock_info = {
    'AAPL': {'exchange': 'NASDAQ', 'base_price': 150},
    'MSFT': {'exchange': 'NASDAQ', 'base_price': 300},
    'GOOGL': {'exchange': 'NASDAQ', 'base_price': 2800},
    'AMZN': {'exchange': 'NASDAQ', 'base_price': 3500},
    'TSLA': {'exchange': 'NASDAQ', 'base_price': 700},
    'FB': {'exchange': 'NASDAQ', 'base_price': 330},
    'NVDA': {'exchange': 'NASDAQ', 'base_price': 200},
    'PYPL': {'exchange': 'NASDAQ', 'base_price': 250},
    'ADBE': {'exchange': 'NASDAQ', 'base_price': 600},
    'NFLX': {'exchange': 'NASDAQ', 'base_price': 500},
    'BRK.B': {'exchange': 'NYSE', 'base_price': 280},
    'JNJ': {'exchange': 'NYSE', 'base_price': 170},
    'V': {'exchange': 'NYSE', 'base_price': 220},
    'WMT': {'exchange': 'NYSE', 'base_price': 140},
    'PG': {'exchange': 'NYSE', 'base_price': 140},
    'DIS': {'exchange': 'NYSE', 'base_price': 180},
    'MA': {'exchange': 'NYSE', 'base_price': 350},
    'KO': {'exchange': 'NYSE', 'base_price': 55},
    'PEP': {'exchange': 'NYSE', 'base_price': 150},
    'MCD': {'exchange': 'NYSE', 'base_price': 250},
}

time_points = pd.date_range(start=start_date, end=end_date, freq='min')
daily_base_prices = {}
for stock in stock_info:
    prices = [stock_info[stock]['base_price']]
    for _ in range(1, len(time_points)):
        minute_return = random.uniform(-0.005, 0.005)
        new_price = prices[-1] * (1 + minute_return)
        prices.append(round(new_price, 2))
    daily_base_prices[stock] = prices

num_records = 10000
records = []
transaction_counter = 1

missing_prob = {
    'price': 0.01,
    'quantity': 0.01,
    'order_type': 0.01,
    'exchange': 0.01
}

for _ in range(num_records):
    # Select a random time within market hours
    random_seconds = random.randint(0, trading_seconds)
    timestamp = start_date + timedelta(seconds=random_seconds)


    stock_symbol = random.choice(list(stock_info.keys()))

    minute_index = min(range(len(time_points)), key=lambda i: abs(time_points[i] - timestamp))
    base_price = daily_base_prices[stock_symbol][minute_index]


    price = base_price * (1 + random.uniform(-0.01, 0.01))  # ±1% variation
    price = None if random.random() < missing_prob['price'] else round(price, 2)
    quantity = None if random.random() < missing_prob['quantity'] else random.randint(1, 1000)
    order_type = None if random.random() < missing_prob['order_type'] else random.choice(['buy', 'sell'])
    exchange = stock_info[stock_symbol]['exchange']
    exchange = None if random.random() < missing_prob['exchange'] else exchange
    transaction_id = f'tx_{transaction_counter:07d}'
    transaction_counter += 1

    # Create record
    record = {
        'transaction_id': transaction_id,
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'stock_symbol': stock_symbol,
        'price': price,
        'quantity': quantity,
        'order_type': order_type,
        'exchange': exchange,
    }
    records.append(record)

df = pd.DataFrame(records)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp').reset_index(drop=True)

df.to_csv(f'../flask/data/stock_transactions_{current_year}_{current_month}_{current_day}.csv', index=False)
