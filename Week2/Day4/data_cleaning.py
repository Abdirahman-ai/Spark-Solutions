from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DataCleaning") \
    .getOrCreate()

# Read tab-separated CSV file
df = spark.read.csv(
    "data1.csv",
    header=True,
    inferSchema=True,
    sep="\t",
    nullValue="null"
)

df.printSchema()
df.show()

# Create temporary SQL view
df.createOrReplaceTempView("data1")

# Remove duplicate rows
unique_df = df.dropDuplicates()
unique_df.show()

# Calculate average age
avg_age = unique_df.select(F.avg("age")).first()[0]

# Fill missing ages with average age
df_filled_age = unique_df.fillna(
    int(avg_age),
    subset=["age"]
)

df_filled_age.show()

# Replace missing names with "Unknown"
df_filled_names = df_filled_age.na.fill(
    "Unknown",
    subset=["name"]
)

df_filled_names.show()

# Filter people over 30
people_over_30 = df_filled_names.filter(
    F.col("age") > 30
)

people_over_30.show()

spark.stop()