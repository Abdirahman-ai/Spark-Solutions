# Goal: With the provided ‘access_lot.txt’ file:
# List IP addresses + sum of their requests
# List requested endpoints + count of number of time they have been called
# List HTTP status codes + count from all requests

from pyspark.sql import SparkSession, functions as func

# ============================================================
# Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("LogAnalysis")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Load Access Log File
# ============================================================

input_df = spark.read.text("./access_log.txt")

# ============================================================
# 1. List IP Addresses and Count Their Requests
# ============================================================

ip_request_counts = (
    input_df
    .select(func.split("value", " ").getItem(0).alias("ip_address"))
    .groupBy("ip_address")
    .count()
    .orderBy("count", ascending=False)
)

print("\nIP Address Request Counts:")
ip_request_counts.show(truncate=False)


# ============================================================
# 2. List Requested Endpoints and Count Calls
# ============================================================

endpoint_counts = (
    input_df
    .select(func.split("value", " ").getItem(6).alias("endpoint"))
    .groupBy("endpoint")
    .count()
    .orderBy("count", ascending=False)
)

print("\nEndpoint Request Counts:")
endpoint_counts.show(truncate=False)

# ============================================================
# 3. List HTTP Status Codes and Count Requests
# ============================================================

status_code_counts = (
    input_df
    .select(func.split("value", " ").getItem(8).alias("status_code"))
    .groupBy("status_code")
    .count()
    .orderBy("count", ascending=False)
)

print("\nHTTP Status Code Counts:")
status_code_counts.show(truncate=False)

# ============================================================
# Stop Spark Session
# ============================================================

spark.stop()