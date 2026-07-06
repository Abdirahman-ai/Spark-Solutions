# Goal:
# Find the top 10 most popular movies in the ML-100k movielens dataset
# Hint:
# You will need a schema, and this file uses ‘Tab’ as separators, so we specify that as an option:
# moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")
# Only return 10 rows:
# topMoviesIDs.show(10)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# ============================================================
# Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Top10MostPopularMovies")
    .getOrCreate()
)

# ============================================================
# Define Schema for MovieLens u.data
# Columns: user_id, movie_id, rating, timestamp
# ============================================================

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])


# ============================================================
# Load MovieLens Ratings Data
# File is tab-separated, so sep is set to "\t"
# ============================================================

movies_df = (
    spark.read
    .option("sep", "\t")
    .schema(schema)
    .csv("ml-100k/u.data")
)


# ============================================================
# Find Top 10 Most Popular Movies
# Popularity is based on number of ratings per movie
# ============================================================

top_10_movies = (
    movies_df
    .groupBy("movie_id")
    .count()
    .orderBy("count", ascending=False)
)


# ============================================================
# Display Results
# ============================================================

print("\nTop 10 Most Popular Movie IDs:")
top_10_movies.show(10)

# ============================================================
# Stop Spark Session
# ============================================================

spark.stop()
