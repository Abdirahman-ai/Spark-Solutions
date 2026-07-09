# 6. Window Function Challenge

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("WindowFunctionChallenge").getOrCreate()

df = spark.read.csv('transactions.csv', header=True, inferSchema=True, sep="\t")
df.printSchema()
df.show()


# Running total
user_window = Window \
    .partitionBy("user") \
    .orderBy("date")

running_total_df = df.withColumn(
    "running_total",
    F.sum("amount").over(user_window)
)

running_total_df.show()

# Previous transaction (Use lag it is a  window function that allows you to access data from a previous row in your DataFrame)
previous_transaction_df = df.withColumn(
    "previous_transaction",
    F.lag("amount").over(user_window)
)
previous_transaction_df.show()

# Difference from previous transaction
difference_df = previous_transaction_df.withColumn(
    "difference",
    F.col("amount") - F.col("previous_transaction")
)

difference_df.show()

# Rolling 7-day average
# first convert the date text
df = df.withColumn(
    "date",
    F.to_date(
        F.concat(F.lit("2026-"), F.col("date")),
        "yyyy-MMMd"
    )
)

# create a numeric timestamp column for the time-range window:
df = df.withColumn(
    "date_seconds",
    F.col("date").cast("timestamp").cast("long")
)

# Define a 7-day window:
seven_day_window = Window \
    .partitionBy("user") \
    .orderBy("date_seconds") \
    .rangeBetween(-6 * 86400, 0)


# Then calculate the rolling average
rolling_avg_df = df.withColumn(
    "rolling_7_day_avg",
    F.round(
        F.avg("amount").over(seven_day_window),
        2
    )
)

rolling_avg_df.show()