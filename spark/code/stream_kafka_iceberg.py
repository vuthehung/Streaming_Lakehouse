from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import time

def run_iceberg_streaming_job():
    spark = SparkSession \
        .builder \
        .appName("KafkaToIcebergStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession đã được khởi tạo.")

    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("stock_symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_type", StringType(), True),
        StructField("exchange", StringType(), True)
    ])
    array_schema = ArrayType(transaction_schema)

    topic = f'stock_transactions_{current_year}_{current_month}_{current_day}'

    kafka_df = None
    max_retries = 3
    retry_delay_seconds = 10

    for attempt in range(max_retries):
        try:
            print(f"Đang thử kết nối Kafka topic '{topic}', lần thử: {attempt + 1}/{max_retries}")
            kafka_df = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()

            print(f"Kết nối Kafka topic '{topic}' thành công.")
            break
        except Exception as e:
            print(f"Không thể kết nối Kafka topic '{topic}': {e}")
            if attempt < max_retries - 1:
                print(f"Đang đợi {retry_delay_seconds} giây trước khi thử lại...")
                time.sleep(retry_delay_seconds)
            else:
                print(f"Không thể kết nối Kafka topic.")
                spark.stop()
                return

    if kafka_df is None:
        print("Không thể khởi tạo Kafka DataFrame, dừng job.")
        spark.stop()
        return

    processed_df = kafka_df \
        .select(col("value").cast("string")) \
        .select(from_json(col("value"), array_schema).alias("transactions")) \
        .select(explode(col("transactions")).alias("transaction")) \
        .select("transaction.*")

    final_df = processed_df \
        .withColumn("ts_year", year(col("timestamp"))) \
        .withColumn("ts_month", month(col("timestamp"))) \
        .withColumn("ts_day", day(col("timestamp"))) \
        .withColumnRenamed("timestamp", "ts")

    print("Stream vào bảng Iceberg: iceberg.stocks.transactions")

    table_name = "iceberg.stocks.transactions"

    checkpoint_location = f's3a://warehouse/checkpoints/stock_transactions_{current_year}_{current_month}_{current_day}_checkpoint'

    query = final_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(table_name)

    query.awaitTermination()
    spark.stop()

if __name__ == '__main__':
    run_iceberg_streaming_job()