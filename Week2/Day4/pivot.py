# 8. Pivot Challenge
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("pivotChallenge").getOrCreate()

# load data file 
df = spark.read.csv("data2.csv", header=True, inferSchema=True, sep="\t")

# apply pivot to transpose data from a long format (rows) 
# to a wide format (columns) by rotating unique values 
# of a specific column into new column headers
pivot_df = df.groupBy("Month").pivot("Product").sum("Sales")
pivot_df.show()