from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Create Spark session
spark = SparkSession.builder \
    .appName("Joins") \
    .getOrCreate()


# Load customers data
customers_df = spark.read.csv(
    "customers.csv",
    header=True,
    inferSchema=True,
    sep="\t"
)

customers_df.printSchema()
customers_df.show()


# Load orders data
orders_df = spark.read.csv(
    "orders.csv",
    header=True,
    inferSchema=True,
    sep="\t"
)

orders_df.printSchema()
orders_df.show()


# Inner join: customers with matching orders
inner_join_df = customers_df.join(
    orders_df,
    customers_df["id"] == orders_df["customer_id"],
    how="inner"
)

inner_join_df.show()


# Left join: all customers, including those without orders
left_join_df = customers_df.join(
    orders_df,
    customers_df["id"] == orders_df["customer_id"],
    how="left"
)

left_join_df.show()


# Customers without orders
customers_without_orders = left_join_df.filter(
    F.col("customer_id").isNull()
)

customers_without_orders.show()


# Total spending per customer
total_spending = inner_join_df.groupBy(
    "id",
    "name"
).agg(
    F.sum("amount").alias("total_spending")
)

total_spending.show()


# Stop Spark session
spark.stop()