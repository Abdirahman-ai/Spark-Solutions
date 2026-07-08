from pyspark.sql import SparkSession, functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
)

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("MovieSimilarities1MDF")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Configuration
# ============================================================

LOCAL_TEST_MODE = False  # Set True for local testing, False for EMR

MOVIE_ID = 260  # Star Wars: Episode IV - A New Hope (1977)

SCORE_THRESHOLD = 0.97
CO_OCCURRENCE_THRESHOLD = 5 if LOCAL_TEST_MODE else 50

# MOVIES_PATH = "../ml-1m/movies.dat"
# RATINGS_PATH = "../ml-1m/ratings.dat"
# OUTPUT_PATH = "../output/movie-sims-parquet"

# ----------------------------
# S3 paths (CHANGE THIS)
# ----------------------------

MOVIES_PATH = "s3://spark-application-024149452378-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3://spark-application-024149452378-us-east-2-an/ml-1m/ratings.dat"
OUTPUT_PATH = "s3://spark-application-024149452378-us-east-2-an/output/movie-sims-df-parquet"


# ============================================================
# Schemas
# ============================================================

ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True),
])

movies_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
])


# ============================================================
# Load MovieLens 1M Data
# ============================================================

ratings_df = (
    spark.read
    .option("sep", "::")
    .schema(ratings_schema)
    .csv(RATINGS_PATH)
)

movies_df = (
    spark.read
    .option("sep", "::")
    .schema(movies_schema)
    .csv(MOVIES_PATH)
)

if LOCAL_TEST_MODE:
    ratings_df = ratings_df.filter(F.col("user_id") <= 100)


# ============================================================
# Create Movie Rating Pairs
# ============================================================

ratings_1 = ratings_df.select(
    "user_id",
    F.col("movie_id").alias("movie_1"),
    F.col("rating").alias("rating_1")
)

ratings_2 = ratings_df.select(
    "user_id",
    F.col("movie_id").alias("movie_2"),
    F.col("rating").alias("rating_2")
)

movie_pairs = (
    ratings_1
    .join(ratings_2, on="user_id")
    .filter(F.col("movie_1") < F.col("movie_2"))
)


# ============================================================
# Calculate Cosine Similarity Components
# ============================================================

pair_scores = (
    movie_pairs
    .groupBy("movie_1", "movie_2")
    .agg(
        F.sum(F.col("rating_1") * F.col("rating_2")).alias("sum_xy"),
        F.sum(F.col("rating_1") * F.col("rating_1")).alias("sum_xx"),
        F.sum(F.col("rating_2") * F.col("rating_2")).alias("sum_yy"),
        F.count("*").alias("num_pairs")
    )
)


# ============================================================
# Calculate Movie Similarities
# ============================================================

movie_pair_similarities = (
    pair_scores
    .withColumn(
        "score",
        F.col("sum_xy") /
        (
            F.sqrt(F.col("sum_xx")) *
            F.sqrt(F.col("sum_yy"))
        )
    )
    .select(
        "movie_1",
        "movie_2",
        "score",
        "num_pairs"
    )
)


# ============================================================
# Save Similarity Results as Parquet
# ============================================================

movie_pair_similarities.write.mode("overwrite").parquet(OUTPUT_PATH)


similarities_df = spark.read.parquet(OUTPUT_PATH)

print(f"\nMovie similarity results saved to: {OUTPUT_PATH}")



# ============================================================
# Find Top Similar Movies for Selected Movie
# ============================================================

filtered_results = (
    similarities_df
    .filter(
        (
            (F.col("movie_1") == MOVIE_ID) |
            (F.col("movie_2") == MOVIE_ID)
        )
        &
        (F.col("score") > SCORE_THRESHOLD)
        &
        (F.col("num_pairs") > CO_OCCURRENCE_THRESHOLD)
    )
)

results_with_similar_id = filtered_results.withColumn(
    "similar_movie_id",
    F.when(F.col("movie_1") == MOVIE_ID, F.col("movie_2"))
    .otherwise(F.col("movie_1"))
)

top_similar_movies = (
    results_with_similar_id
    .join(
        movies_df,
        results_with_similar_id["similar_movie_id"] == movies_df["movie_id"]
    )
    .select(
        "similar_movie_id",
        "title",
        F.round("score", 4).alias("similarity_score"),
        F.col("num_pairs").alias("co_rating_strength")
    )
    .orderBy(
        F.desc("similarity_score"),
        F.desc("co_rating_strength")
    )
)


# ============================================================
# Display Results
# ============================================================

movie_title = (
    movies_df
    .filter(F.col("movie_id") == MOVIE_ID)
    .select("title")
    .first()[0]
)

print(f"\nRunning in local test mode: {LOCAL_TEST_MODE}")
# top_similar_movies.show(10, truncate=False)

results = top_similar_movies.take(10)

print(f"\nTop 10 similar movies for: {movie_title}")

for row in results:
    print(
        f"{row['title']} \t"
        f"score: {row['similarity_score']} \t"
        f"strength: {row['co_rating_strength']}"
    )


# ============================================================
# Stop Spark Session
# ============================================================

spark.stop()