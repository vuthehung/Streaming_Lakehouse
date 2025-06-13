from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import argparse


def run_bronze_to_silver_job(bronze_table, silver_table):
    spark = SparkSession \
        .builder \
        .appName("PreProcessing") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"SparkSession đã được khởi tạo.")

    current_year = 2025
    current_month = 6
    current_day = 7
    bronze_df = spark.read.table(bronze_table)
    filtered_df = bronze_df.filter(
        (col('ts_year') == current_year) &
        (col('ts_month') == current_month) &
        (col('ts_day') == current_day)
    )
    print(f"Đã đọc {filtered_df.count()} bản ghi.")
    if filtered_df.count() == 0:
        print("Bảng không có dữ liệu mới. Kết thúc job.")
        spark.stop()
        return

    # Xử lý NULL
    cleaned_df = filtered_df.na.drop(subset=["price", "quantity", "order_type", "exchange"])
    print(f"Số bản ghi sau khi loại bỏ NULL: {cleaned_df.count()}.")

    # Loại bỏ TRÙNG LẶP (Deduplication)
    window_spec = Window.partitionBy("transaction_id").orderBy(col("ts").desc())

    dedup_df = cleaned_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    print(f"Số bản ghi sau khi loại bỏ trùng lặp: {dedup_df.count()}.")

    # Tạo một temporary view để sử dụng trong câu lệnh MERGE
    dedup_df.createOrReplaceTempView("silver_updates")

    # MERGE dữ liệu vào bảng cleaned
    # - Nếu transaction_id đã tồn tại trong bảng cleaned, nó sẽ được update.
    # - Nếu transaction_id chưa có, nó sẽ được insert.
    merge_query = f"""
    MERGE INTO {silver_table} t
    USING silver_updates u
    ON t.transaction_id = u.transaction_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """

    print("Đang thực hiện lệnh MERGE INTO ")
    spark.sql(merge_query)
    print("Hoàn tất việc merge.")

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bronze_table',
        type=str,
        default="iceberg.stocks.transactions",
    )
    parser.add_argument(
        '--silver_table',
        type=str,
        default="iceberg.stocks.transactions_cleaned",
    )
    args = parser.parse_args()


    run_bronze_to_silver_job(args.bronze_table, args.silver_table)