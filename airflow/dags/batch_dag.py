from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    'owner': 'hungde',
    'start_date': datetime(2025, 6, 7, 9, 30),
    'retries': 3,
    'retry_delay': 300
}
SQL_CREATE_SCHEMA = 'CREATE SCHEMA IF NOT EXISTS iceberg.stocks_reporting'

SQL_CREATE_TABLE_daily_market_summary = '''
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
)
'''

SQL_CREATE_TABLE_daily_stock_summary = '''
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
)
'''

SQL_CREATE_TABLE_daily_order_type_summary = '''
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
)
'''

SQL_CREATE_TABLE_daily_exchange_summary = '''
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
)
'''


SQL_INSERT_daily_market_summary = '''
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
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE)
'''

SQL_INSERT_daily_stock_summary = '''
INSERT INTO iceberg.stocks_reporting.daily_stock_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    stock_symbol,
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
    ts_year, ts_month, CAST(ts, 'yyyy-MM-dd') AS DATE), stock_symbol
'''

SQL_INSERT_daily_order_type_summary = '''
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
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), order_type
'''

SQL_INSERT_daily_exchange_summary = '''
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
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), exchange
'''

with DAG(
    'batch_dag',
     default_args=default_args,
     schedule_interval='@hourly',
     max_active_runs=1,
     catchup=False
) as dag:
    create_schema_task = TrinoOperator(
        task_id="create_schema_reporting",
        trino_conn_id="trino_conn",
        sql=SQL_CREATE_SCHEMA,
    )
    create_daily_market_summary_task = TrinoOperator(
        task_id="create_daily_market_summary",
        trino_conn_id="trino_conn",
        sql=SQL_CREATE_TABLE_daily_market_summary,
    )
    create_daily_stock_summary_task = TrinoOperator(
        task_id="create_daily_stock_summary",
        trino_conn_id="trino_conn",
        sql=SQL_CREATE_TABLE_daily_stock_summary,
    )
    create_daily_order_type_summary_task = TrinoOperator(
        task_id="create_daily_order_type_summary",
        trino_conn_id="trino_conn",
        sql=SQL_CREATE_TABLE_daily_order_type_summary,
    )
    create_daily_exchange_summary_task = TrinoOperator(
        task_id="create_daily_exchange_summary",
        trino_conn_id="trino_conn",
        sql=SQL_CREATE_TABLE_daily_exchange_summary,
    )
    insert_daily_market_summary_task = TrinoOperator(
        task_id="insert_daily_market_summary",
        trino_conn_id="trino_conn",
        sql=SQL_INSERT_daily_market_summary,
    )
    insert_daily_stock_summary_task = TrinoOperator(
        task_id="insert_daily_stock_summary",
        trino_conn_id="trino_conn",
        sql=SQL_INSERT_daily_stock_summary,
    )
    insert_daily_order_type_summary_task = TrinoOperator(
        task_id="insert_daily_order_type_summary",
        trino_conn_id="trino_conn",
        sql=SQL_INSERT_daily_order_type_summary,
    )
    insert_daily_exchange_summary_task = TrinoOperator(
        task_id="insert_daily_exchange_summary",
        trino_conn_id="trino_conn",
        sql=SQL_INSERT_daily_exchange_summary,
    )
    clean_data_job = SparkSubmitOperator(
        task_id="clean_data",
        application="/opt/airflow/code/clean_data.py",
        conn_id="spark_conn",
        verbose=True,
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    )

    clean_data_job >> create_schema_task

    create_schema_task >> [
        create_daily_market_summary_task,
        create_daily_stock_summary_task,
        create_daily_order_type_summary_task,
        create_daily_exchange_summary_task
    ]

    create_daily_market_summary_task >> insert_daily_market_summary_task
    create_daily_stock_summary_task >> insert_daily_stock_summary_task
    create_daily_order_type_summary_task >> insert_daily_order_type_summary_task
    create_daily_exchange_summary_task >> insert_daily_exchange_summary_task