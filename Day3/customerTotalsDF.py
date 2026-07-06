# Goal: Print the id for a customer and 
# the total of all their orders from ‘customer-orders.csv’ 
# in descending order
from pyspark.sql import SparkSession
from pyspark.sql.functions import round

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CustomerAndTotalsDf")
    .getOrCreate()
)

# Load headerless CSV and assign column names
df = (
    spark.read
    .option("inferSchema", True)
    .csv("customer-orders.csv")
    .toDF("customer_id", "order_id", "total_amount")
)

# Group by customer_id, total the order amounts, and sort descending
customer_totals = (
    df.groupBy("customer_id")
    .sum("total_amount")
    .withColumnRenamed("sum(total_amount)", "total_spent")
    .withColumn("total_spent", round("total_spent", 2))
    .orderBy("total_spent", ascending=False)
)

customer_totals.show()

spark.stop()