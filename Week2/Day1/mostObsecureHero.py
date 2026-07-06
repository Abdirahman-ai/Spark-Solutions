# Goal:
# Find all obscure heroes (there may be ties)
# Hint code snippets:
# Dataframename.agg(func.min(‘columnName’).first()[0]
# Dataframename.filter(func.col(‘columnName’) == someValue)
# Dataframename.join(otherDataframeWithACommonColumnName, ‘commonColumnName’)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = (
    SparkSession.builder
    .appName("SuperHeroes")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema for MarvelNames.txt
names_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

# Load hero names
names_df = (
    spark.read
    .schema(names_schema)
    .option("sep", " ")
    .csv("./MarvelNames.txt")
)

# Load hero connection graph
lines = spark.read.text("./MarvelGraph.txt")

# Count total connections for each hero ID
connections = (
    lines
    .withColumn("id", F.split(F.col("value"), " ")[0].cast("int"))
    .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(F.sum("connections").alias("connections"))
)

minimum_value = connections.agg(F.min("connections")).first()[0]

obscure_heroes = connections.filter(F.col("connections") == minimum_value)

obscure_heroes_with_names = obscure_heroes.join(names_df, "id")

obscure_heroes_with_names.show(truncate=False)