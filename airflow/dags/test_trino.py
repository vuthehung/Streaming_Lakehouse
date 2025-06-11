from datetime import datetime
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    'owner': 'hungde',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

SOURCE_TABLE = 'iceberg.data_db.trips'
TARGET_TABLE = 'iceberg.data_db.vendor_trip_summary'

# Câu lệnh CTAS (CREATE TABLE AS SELECT) để tính toán và tạo bảng mới.
# - COUNT(*): Đếm tổng số chuyến đi.
# - SUM(trip_distance): Tính tổng quãng đường.
# - AVG(fare_amount): Tính phí di chuyển trung bình.
SQL_CREATE_SUMMARY_TABLE = f"""
CREATE OR REPLACE TABLE {TARGET_TABLE} AS
SELECT
    vendor_id,
    COUNT(*) AS total_trips,
    SUM(trip_distance) AS total_distance,
    AVG(fare_amount) AS average_fare
FROM
    {SOURCE_TABLE}
GROUP BY
    vendor_id
"""

with DAG(
    'test_trino',
     default_args=default_args,
     schedule_interval='@daily',
     catchup=False
) as dag:
    create_summary_table = TrinoOperator(
        task_id="create_summary_table_task",
        trino_conn_id="trino_conn",
        sql=SQL_CREATE_SUMMARY_TABLE,
    )