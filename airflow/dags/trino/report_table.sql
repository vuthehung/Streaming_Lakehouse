CREATE SCHEMA IF NOT EXISTS iceberg.stocks_reporting;

-- Bảng tổng hợp khối lượng và giá trị giao dịch toàn thị trường theo ngày
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_market_summary (
    report_date DATE,
    total_volume BIGINT,
    total_value DOUBLE,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

INSERT INTO iceberg.stocks_reporting.daily_market_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    SUM(quantity) AS total_volume,
    SUM(CAST(price AS DOUBLE) * quantity) AS total_value,
    ts_year AS year,
    ts_month AS month
FROM
    iceberg.stocks.transactions
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE);

-- Bảng tổng hợp các chỉ số giao dịch theo từng mã cổ phiếu hàng ngày
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_stock_summary (
    report_date DATE,
    stock_symbol VARCHAR,
    exchange VARCHAR,
    total_volume BIGINT,
    total_value DOUBLE,
    transaction_count BIGINT,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

INSERT INTO iceberg.stocks_reporting.daily_stock_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    stock_symbol,
    exchange,
    SUM(quantity) AS total_volume,
    SUM(CAST(price AS DOUBLE) * quantity) AS total_value,
    COUNT(transaction_id) AS transaction_count,
    ts_year AS year,
    ts_month AS month
FROM
    iceberg.stocks.transactions
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(ts, 'yyyy-MM-dd') AS DATE), stock_symbol, exchange;

-- Bảng tổng hợp số lượng và khối lượng lệnh theo loại Mua/Bán hàng ngày
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_order_type_summary (
    report_date DATE,
    order_type VARCHAR,
    order_count BIGINT,
    total_volume BIGINT,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

INSERT INTO iceberg.stocks_reporting.daily_order_type_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    order_type,
    COUNT(transaction_id) AS order_count,
    SUM(quantity) AS total_volume,
    ts_year AS year,
    ts_month AS month
FROM
    iceberg.stocks.transactions
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), order_type;

--Bảng tổng hợp khối lượng và giá trị giao dịch theo từng sàn hàng ngày
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_exchange_summary (
    report_date DATE,
    exchange VARCHAR,
    total_volume BIGINT,
    total_value DOUBLE,
    transaction_count BIGINT,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

INSERT INTO iceberg.stocks_reporting.daily_exchange_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    exchange,
    SUM(quantity) AS total_volume,
    SUM(CAST(price AS DOUBLE) * quantity) AS total_value,
    COUNT(transaction_id) AS transaction_count,
    year,
    month
FROM
    iceberg.stocks.transactions
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), exchange;