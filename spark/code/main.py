from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType, \
    ArrayType


def run_iceberg_streaming_job():
    spark = SparkSession \
        .builder \
        .appName("KafkaToIcebergStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession đã được khởi tạo.")

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("stock_symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_type", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("minute", TimestampType(), True),
        StructField("volume", LongType(), True)
    ])
    array_schema = ArrayType(transaction_schema)

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "stock_transactions") \
        .option("startingOffsets", "earliest") \
        .load()

    processed_df = kafka_df \
        .select(col("value").cast("string")) \
        .select(from_json(col("value"), array_schema).alias("transactions")) \
        .select(explode(col("transactions")).alias("transaction")) \
        .select("transaction.*")

    print("Chuẩn bị ghi stream vào bảng Iceberg: iceberg.stocks.transactions")

    table_name = "iceberg.stocks.transactions"

    checkpoint_location = "s3a://warehouse/checkpoints/stock_transactions_checkpoint"

    query = processed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .toTable(table_name)

    query.awaitTermination()


if __name__ == '__main__':
    run_iceberg_streaming_job()