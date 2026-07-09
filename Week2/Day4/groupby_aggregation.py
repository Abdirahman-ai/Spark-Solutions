from pyspark.sql import SparkSession, functions as F

# Create Spark session
spark = SparkSession.builder \
    .appName("GroupByAggregation") \
    .getOrCreate()

# Load sales data
df = spark.read.csv(
    "sales.csv",
    header=True,
    inferSchema=True,
    sep="\t"
)

df.printSchema()
df.show()

# Create temporary SQL view
df.createOrReplaceTempView("sales")

# Total spending per customer
total_spend = df.groupBy("customer").agg(
    F.sum("amount").alias("total_spending")
)

total_spend.show()


# Average purchase amount
avg_amount = df.agg(
    F.round(
        F.avg("amount"),
        2
    ).alias("average_purchase_amount")
)

avg_amount.show()


# Highest-spending customer based on total spending
highest_spender = total_spend.orderBy(
    F.col("total_spending").desc()
).limit(1)

highest_spender.show()


# Stop Spark session
spark.stop()