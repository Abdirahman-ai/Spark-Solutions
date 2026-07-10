from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("ExplodeNestedData").getOrCreate()

data = [
    ("Alice", ["Math", "Science"]),
    ("Bob", ["History"])
]

columns = ["name", "subjects"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

df.show()

# Explode the subjects array into individual rows
exploded_df = df.select(
    "name",
    explode("subjects").alias("subject")
)

exploded_df.show()

spark.stop()