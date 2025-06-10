from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-iceberg:7077") \
    .appName("Test") \
    .getOrCreate()

data = [
    ("Rajesh Kumar", "1990-05-14", "ABCDE1234F", "Delhi", "New Delhi", "Connaught Place", "rajesh.k@gmail.com",
     "2024-01-01"),
    ("Rajesh Kumar", "1990-05-14", "ABCDE1234F", "Delhi", "New Delhi", "Karol Bagh", "rajesh.kumar@outlook.com",
     "2024-02-01")
]

columns = ["customer_name", "dob", "pan_number", "state", "city", "branch_name", "email", "updated_at"]

df = spark.createDataFrame(data, columns)
df.show(truncate=False)

spark.stop()
