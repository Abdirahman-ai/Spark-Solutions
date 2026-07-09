from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Create Spark session
spark = SparkSession.builder \
    .appName("EmailDuplicates") \
    .getOrCreate()


# Load email data
df = spark.read.csv(
    "emails.csv",
    header=True,
    inferSchema=True
)

# Remove separator row from the dataset
df = df.filter(
    F.col("email") != "-----"
)

df.show()


# Find duplicate emails
duplicate_emails = df.groupBy("email") \
    .count() \
    .filter(F.col("count") > 1)

duplicate_emails.show()


# Count occurrences of each email
email_occurrences = df.groupBy("email") \
    .count()

email_occurrences.show()


# Keep only emails that appear exactly once
unique_emails = df.groupBy("email") \
    .count() \
    .filter(F.col("count") == 1) \
    .select("email")

unique_emails.show()


# Stop Spark session
spark.stop()