from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def run_iceberg_streaming_job():
    spark = SparkSession \
        .builder \
        .appName("KafkaToIcebergStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession đã được khởi tạo.")

    current_year = 2025
    current_month = 6
    current_day = 7
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
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

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

    print("Chuẩn bị ghi stream vào bảng Iceberg: iceberg.stocks.transactions")

    table_name = "iceberg.stocks.transactions"

    checkpoint_location = f's3a://warehouse/checkpoints/stock_transactions_{current_year}_{current_month}_{current_day}_checkpoint'

    query = final_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(table_name)

    query.awaitTermination()
    print("Micro-batch đã xử lý xong. Dừng SparkSession.")
    spark.stop()

if __name__ == '__main__':
    run_iceberg_streaming_job()