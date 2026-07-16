# Import the SparkSession class, which is the entry point for working with Spark.
from pyspark.sql import SparkSession


# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("TitanicToIceberg")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://spark-application-024149452378-us-east-2-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)


# Read the Titanic dataset from a Parquet file stored in S3
# and load it into a Spark DataFrame.
titanic_df = spark.read.parquet(
    "s3://spark-application-024149452378-us-east-2-an/titanic.parquet"
)

# Display the DataFrame's schema (column names and data types)
# to verify the data was loaded correctly.
titanic_df.printSchema()


# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_demo
""")


# Write the DataFrame as an Iceberg table.
(
    titanic_df.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_demo.titanic"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)


# Query the newly created Iceberg table to verify that the
# data was written successfully.
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_demo.titanic
LIMIT 10
""").show()


# Stop the Spark session and release cluster resources.
spark.stop()