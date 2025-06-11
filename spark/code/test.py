from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def run_iceberg_test_job():
    spark = SparkSession.builder \
        .appName("IcebergMinioWriteTest") \
        .getOrCreate()

    print("--- SparkSession đã được khởi tạo thành công ---")

    # 2. Tạo dữ liệu mẫu
    # Tạo một DataFrame nhỏ để ghi vào Iceberg.
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True)
    ])

    data = [
        (1, "Sản phẩm A", "Thiết bị điện tử"),
        (2, "Sản phẩm B", "Đồ gia dụng"),
        (3, "Sản phẩm C", "Thiết bị điện tử")
    ]

    df = spark.createDataFrame(data, schema)

    print("--- Dữ liệu mẫu đã được tạo: ---")
    df.show()

    table_name = "iceberg.data_db.table"

    print(f"--- Bắt đầu ghi dữ liệu vào bảng: {table_name} ---")
    df.writeTo(table_name).createOrReplace()
    print(f"--- Ghi dữ liệu thành công! ---")

    print(f"--- Đọc lại dữ liệu từ bảng {table_name} để xác thực: ---")
    read_df = spark.table(table_name)
    read_df.show()

    print("--- Job hoàn thành. Dừng SparkSession. ---")
    spark.stop()

if __name__ == "__main__":
    run_iceberg_test_job()