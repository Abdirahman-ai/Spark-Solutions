from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Create Spark session
spark = SparkSession.builder \
    .appName("HighestPaidEmployee") \
    .getOrCreate()


# Load employee data
df = spark.read.csv(
    "employee.csv",
    header=True,
    inferSchema=True,
    sep="\t"
)

df.printSchema()
df.show()


# Define window ordered by salary from highest to lowest
salary_window = Window.orderBy(
    F.col("salary").desc()
)


# Rank employees by salary
ranked_employees = df.withColumn(
    "salary_rank",
    F.dense_rank().over(salary_window)
)

ranked_employees.show()


# Get the highest-paid employee(s)
highest_paid_employees = ranked_employees.filter(
    F.col("salary_rank") == 1
)

highest_paid_employees.show()


# Stop Spark session
spark.stop()