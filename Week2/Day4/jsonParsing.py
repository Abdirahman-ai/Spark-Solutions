from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("JsonParsingChallenge").getOrCreate()


# Read multi-line JSON file
df = spark.read.option("multiline", True).json("data3.json")

df.printSchema()
df.show(truncate=False)


# Flatten nested customer fields
flattened_df = df.select(
    F.col("customer.id").alias("customer_id"),
    F.col("customer.city").alias("customer_city")
)

flattened_df.show()


spark.stop()