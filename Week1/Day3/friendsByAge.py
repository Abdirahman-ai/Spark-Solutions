# Challenge – Friends by age with dataframes
# Goal: Print a dataframe of the name and age for each entry
# of fakefriends-header.csv
from pyspark.sql import SparkSession

# ============================================================
# Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("FriendsByAge")
    .getOrCreate()
)

# ============================================================
# Load Friends Data
# ============================================================

friends = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("fakefriends-header.csv")
)

# ============================================================
# Select Name and Age Columns
# ============================================================

name_and_age = friends.select("name", "age")

# ============================================================
# Display Results
# ============================================================

print("\nNames and Ages:")
name_and_age.show()

# ============================================================
# Stop Spark Session
# ============================================================

spark.stop()