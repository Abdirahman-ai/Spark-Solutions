# Import the SparkSession class, which is the entry point for working with Spark.
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StructField, StringType, IntegerType, StructType, DateType, DecimalType
from pyspark.sql.functions import col, when, lower, upper
import pyspark.sql.functions as F

# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("CustomerOrderToIceberg")

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

schema = StructType(
    # customer_id,first_name,last_name,email,phone,signup_date,country,state,postal_code,is_active,loyalty_points
    [
        StructField("customer_id", IntegerType(), True), # NEED TO REVISIT THIS AS 1ROW CONTAINS XYZ CUSTOMER_ID
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("loyalty_points", DecimalType(10, 2), True),
    ]
)
# df = SparkSession.read.option().csv("s3://spark-application-024149452378-us-east-2-an/project1/customers.csv")
customer_df = spark.read.option("header", "true").schema(schema).csv("s3://spark-application-024149452378-us-east-2-an/project1/customers.csv")
customer_df.printSchema()
# customer_df.show()

customer_df = customer_df.withColumn(
    "country",
    when(col("country") == "U.S.A.", "USA")
    .when(col("country") == "United States", "USA")
    .when(col("country") == "United States of America", "USA")
    .when(col("country") == "US", "USA")
    .otherwise(col("country"))
)

customer_df = customer_df.withColumn(
    "is_active",
    lower(col("is_active"))
)

customer_df = customer_df.withColumn("is_active", when(col("is_active") == "Y", "True")
                   .when(col("is_active") == "n", "False")
                   .when(col("is_active") == "y", "True")
                   .when(col("is_active") == "yes", "True")
                   .when(col("is_active") == "no", "False")
                   .when(col("is_active") == "1", "True")
                   .when(col("is_active") == "0", "False")
                   .otherwise(col("is_active")))

customer_df = customer_df.withColumn(
    "state",
    lower(col("state")))

customer_df = customer_df.withColumn("state",
                when(col("state") == "minnesota", "MN")
                .otherwise(upper(col("state"))))

customer_df.describe().show()

customer_df.groupBy("country").count().show()

customer_df.groupBy("state").count().show()

customer_df.groupBy("is_active").count().show()

customer_df = customer_df.dropDuplicates()
customer_df = customer_df.na.drop()
# customer_df.show()

email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
customer_df = customer_df.filter(
    customer_df.email.rlike(email_regex)
    )

# customer_df.show()

phone_regex = r"^\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})$"
customer_df = customer_df.filter(
    customer_df.phone.rlike(phone_regex)
    )
customer_df.show()



#############################################################################################################
"""
Products File
"""
#############################################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

#############################################################################################################
# Read dirty product columns as strings
#############################################################################################################

product_schema = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("cost", StringType(), True),
        StructField("stock_quantity", StringType(), True),
        StructField("weight_kg", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("is_active", StringType(), True)
    ]
)

product_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("quote", '"')
    .option("escape", '"')
    .schema(product_schema)
    .csv("s3://spark-application-024149452378-us-east-2-an/project1/products.csv")
)

#############################################################################################################
# Trim all string columns
#############################################################################################################

for column_name in product_df.columns:
    product_df = product_df.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

#############################################################################################################
# Standardize product identifiers and text columns
#############################################################################################################

product_df = (
    product_df
    .withColumn(
        "product_id",
        F.upper(F.col("product_id"))
    )
    .withColumn(
        "product_name",
        F.initcap(F.col("product_name"))
    )
    .withColumn(
        "category",
        F.initcap(F.col("category"))
    )
    .withColumn(
        "brand",
        F.initcap(F.col("brand"))
    )
)

#############################################################################################################
# Clean and cast price
#############################################################################################################

product_df = product_df.withColumn(
    "price",
    F.regexp_replace(
        F.col("price"),
        r"^\$",
        ""
    )
)

product_df = product_df.withColumn(
    "price",
    F.regexp_replace(
        F.col("price"),
        ",",
        "."
    )
)

product_df = product_df.withColumn(
    "price",
    F.expr(
        "try_cast(price AS DECIMAL(10,2))"
    )
)

#############################################################################################################
# Clean and cast cost
#############################################################################################################

product_df = product_df.withColumn(
    "cost",
    F.regexp_replace(
        F.col("cost"),
        r"^\$",
        ""
    )
)

product_df = product_df.withColumn(
    "cost",
    F.regexp_replace(
        F.col("cost"),
        ",",
        "."
    )
)

product_df = product_df.withColumn(
    "cost",
    F.expr(
        "try_cast(cost AS DECIMAL(10,2))"
    )
)

#############################################################################################################
# Clean and cast stock quantity
#############################################################################################################

product_df = product_df.withColumn(
    "stock_quantity",
    F.expr(
        "try_cast(stock_quantity AS INT)"
    )
)

#############################################################################################################
# Clean and cast weight
#############################################################################################################

product_df = product_df.withColumn(
    "weight_kg",
    F.expr(
        "try_cast(weight_kg AS DECIMAL(10,2))"
    )
)

#############################################################################################################
# Parse created date
#############################################################################################################

product_df = product_df.withColumn(
    "created_date",
    F.coalesce(
        F.expr(
            "try_to_timestamp(created_date, 'yyyy-MM-dd')"
        ).cast("date"),
        F.expr(
            "try_to_timestamp(created_date, 'yyyy/MM/dd')"
        ).cast("date"),
        F.expr(
            "try_to_timestamp(created_date, 'MM/dd/yyyy')"
        ).cast("date")
    )
)

#############################################################################################################
# Convert active status to Boolean
#############################################################################################################

product_df = product_df.withColumn(
    "is_active",
    F.when(
        F.lower(F.col("is_active")).isin(
            "true",
            "yes",
            "y",
            "1"
        ),
        True
    )
    .when(
        F.lower(F.col("is_active")).isin(
            "false",
            "no",
            "n",
            "0"
        ),
        False
    )
    .otherwise(None)
)

#############################################################################################################
# Replace negative stock quantities with zero
#############################################################################################################

product_df = product_df.withColumn(
    "stock_quantity",
    F.when(
        F.col("stock_quantity") < 0,
        F.lit(0)
    ).otherwise(F.col("stock_quantity"))
)

#############################################################################################################
# Treat negative prices and costs as invalid
#############################################################################################################

product_df = product_df.withColumn(
    "price",
    F.when(
        F.col("price") >= 0,
        F.col("price")
    ).otherwise(None)
)

product_df = product_df.withColumn(
    "cost",
    F.when(
        F.col("cost") >= 0,
        F.col("cost")
    ).otherwise(None)
)

#############################################################################################################
# Validate product ID format
#
# Valid examples: P1001, P1002, P1020
#############################################################################################################

product_df = product_df.filter(
    F.col("product_id").rlike(r"^P\d{4}$")
)

#############################################################################################################
# Remove duplicate product IDs
#############################################################################################################

product_df = product_df.dropDuplicates(
    ["product_id"]
)

#############################################################################################################
# Remove products with missing required values
#
# weight_kg is not included because P1012 legitimately has no weight.
#############################################################################################################

required_product_columns = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "price",
    "cost",
    "stock_quantity",
    "created_date",
    "is_active"
]

product_df = product_df.na.drop(
    subset=required_product_columns
)

#############################################################################################################
# Restore final column order
#############################################################################################################

product_df = product_df.select(
    "product_id",
    "product_name",
    "category",
    "brand",
    "price",
    "cost",
    "stock_quantity",
    "weight_kg",
    "created_date",
    "is_active"
)

#############################################################################################################
# Display cleaned products
#############################################################################################################

product_df.printSchema()

product_df.orderBy("product_id").show(
    truncate=False
)

#############################################################################################################
"""
Orders File
"""
#############################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

#############################################################################################################
# Create Spark session
#############################################################################################################

spark = (
    SparkSession.builder
    .appName("OrdersDataCleaning")
    .getOrCreate()
)

#############################################################################################################
# Define schema
#############################################################################################################

order_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount_pct", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True)
    ]
)

#############################################################################################################
# Read orders CSV
#############################################################################################################

order_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(order_schema)
    .csv("s3://spark-application-024149452378-us-east-2-an/project1/orders.csv")
)

#############################################################################################################
# Remove completely duplicated rows
#############################################################################################################

order_df = order_df.dropDuplicates()

#############################################################################################################
# Trim all string columns
#############################################################################################################

for column_name in order_df.columns:
    order_df = order_df.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

#############################################################################################################
# Clean and cast IDs
#############################################################################################################

order_df = (
    order_df
    .withColumn(
        "order_id",
        F.expr("try_cast(order_id AS BIGINT)")
    )
    .withColumn(
        "customer_id",
        F.expr("try_cast(customer_id AS BIGINT)")
    )
    .withColumn(
        "product_id",
        F.upper(F.col("product_id"))
    )
)

#############################################################################################################
# Parse order date
#############################################################################################################

order_df = order_df.withColumn(
    "order_date",
    F.coalesce(
        F.expr(
            "try_to_timestamp(order_date, 'yyyy-MM-dd')"
        ).cast("date"),
        F.expr(
            "try_to_timestamp(order_date, 'yyyy/MM/dd')"
        ).cast("date"),
        F.expr(
            "try_to_timestamp(order_date, 'MM/dd/yyyy')"
        ).cast("date")
    )
)

#############################################################################################################
# Parse ship date
#############################################################################################################

order_df = order_df.withColumn(
    "ship_date",
    F.coalesce(
        F.expr(
            "try_to_timestamp(ship_date, 'yyyy-MM-dd')"
        ).cast("date"),
        F.expr(
            "try_to_timestamp(ship_date, 'yyyy/MM/dd')"
        ).cast("date"),
        F.expr(
            "try_to_timestamp(ship_date, 'MM/dd/yyyy')"
        ).cast("date")
    )
)

#############################################################################################################
# Clean and cast quantity
#############################################################################################################

order_df = order_df.withColumn(
    "quantity",
    F.expr("try_cast(quantity AS INT)")
)

#############################################################################################################
# Clean and cast unit price
#############################################################################################################

order_df = order_df.withColumn(
    "unit_price",
    F.when(
        F.upper(F.col("unit_price")).isin(
            "N/A",
            "NA",
            "NULL",
            "NONE",
            ""
        ),
        None
    ).otherwise(F.col("unit_price"))
)

order_df = order_df.withColumn(
    "unit_price",
    F.regexp_replace(
        F.col("unit_price"),
        r"^\$",
        ""
    )
)

order_df = order_df.withColumn(
    "unit_price",
    F.regexp_replace(
        F.col("unit_price"),
        ",",
        "."
    )
)

order_df = order_df.withColumn(
    "unit_price",
    F.expr(
        "try_cast(unit_price AS DECIMAL(10,2))"
    )
)

#############################################################################################################
# Clean and cast discount percentage
#############################################################################################################

order_df = order_df.withColumn(
    "discount_pct",
    F.expr("try_cast(discount_pct AS INT)")
)

#############################################################################################################
# Standardize order status
#############################################################################################################

order_df = order_df.withColumn(
    "order_status",
    F.initcap(
        F.lower(
            F.trim(F.col("order_status"))
        )
    )
)

valid_order_statuses = [
    "Pending",
    "Delivered",
    "Cancelled",
    "Returned",
    "Shipped"
]

order_df = order_df.withColumn(
    "order_status",
    F.when(
        F.col("order_status").isin(valid_order_statuses),
        F.col("order_status")
    ).otherwise(None)
)

#############################################################################################################
# Validate quantity
#############################################################################################################

order_df = order_df.withColumn(
    "quantity",
    F.when(
        (F.col("quantity") == 0)
        & (F.col("order_status") == "Cancelled"),
        F.col("quantity")
    )
    .when(
        F.col("quantity") > 0,
        F.col("quantity")
    )
    .otherwise(None)
)

#############################################################################################################
# Validate unit price
#############################################################################################################

order_df = order_df.withColumn(
    "unit_price",
    F.when(
        F.col("unit_price") >= 0,
        F.col("unit_price")
    ).otherwise(None)
)

#############################################################################################################
# Validate discount percentage
#############################################################################################################

order_df = order_df.withColumn(
    "discount_pct",
    F.when(
        F.col("discount_pct").isNull(),
        F.lit(0)
    )
    .when(
        F.col("discount_pct").between(0, 100),
        F.col("discount_pct")
    )
    .otherwise(None)
)

#############################################################################################################
# Standardize payment methods
#############################################################################################################

clean_payment_method = F.lower(
    F.trim(F.col("payment_method"))
)

order_df = order_df.withColumn(
    "payment_method",
    F.when(
        clean_payment_method.isin(
            "credit card",
            "visa",
            "mastercard",
            "master card"
        ),
        "Credit Card"
    )
    .when(
        clean_payment_method.isin(
            "debit",
            "debit card"
        ),
        "Debit Card"
    )
    .when(
        clean_payment_method == "paypal",
        "PayPal"
    )
    .when(
        clean_payment_method == "apple pay",
        "Apple Pay"
    )
    .when(
        clean_payment_method == "google pay",
        "Google Pay"
    )
    .when(
        clean_payment_method == "cash",
        "Cash"
    )
    .otherwise(None)
)

#############################################################################################################
# Validate ship date
#############################################################################################################

order_df = order_df.withColumn(
    "ship_date",
    F.when(
        F.col("order_date").isNotNull()
        & F.col("ship_date").isNotNull()
        & (F.col("ship_date") >= F.col("order_date")),
        F.col("ship_date")
    ).otherwise(None)
)

#############################################################################################################
# Recalculate total amount
#############################################################################################################

order_df = order_df.withColumn(
    "total_amount",
    F.when(
        F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("discount_pct").isNotNull(),
        F.round(
            F.col("quantity")
            * F.col("unit_price")
            * (
                F.lit(1.0)
                - (
                    F.col("discount_pct").cast("double")
                    / F.lit(100.0)
                )
            ),
            2
        ).cast(DecimalType(12, 2))
    ).otherwise(None)
)

#############################################################################################################
# Remove duplicate order IDs
#############################################################################################################

order_df = order_df.dropDuplicates(["order_id"])

#############################################################################################################
# Remove rows containing invalid or missing required values
#############################################################################################################

required_columns = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "ship_date",
    "quantity",
    "unit_price",
    "discount_pct",
    "total_amount",
    "payment_method",
    "order_status"
]

order_df = order_df.na.drop(
    subset=required_columns
)

#############################################################################################################
# Validate customer IDs against customer DataFrame
#############################################################################################################

order_df = order_df.join(
    customer_df
    .select(
        F.col("customer_id")
        .cast("bigint")
        .alias("customer_id")
    )
    .dropDuplicates(),
    on="customer_id",
    how="inner"
)

#############################################################################################################
# Validate product IDs against product DataFrame
#############################################################################################################

order_df = order_df.join(
    product_df
    .select(
        F.upper(
            F.trim(F.col("product_id"))
        ).alias("product_id")
    )
    .dropDuplicates(),
    on="product_id",
    how="inner"
)

#############################################################################################################
# Restore final column order
#############################################################################################################

order_df = order_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "ship_date",
    "quantity",
    "unit_price",
    "discount_pct",
    "total_amount",
    "payment_method",
    "order_status"
)

#############################################################################################################
# Display cleaned orders
#############################################################################################################

order_df.printSchema()
order_df.show(truncate=False)




# ============================================================
# Write curated DataFrames to Apache Iceberg
# ============================================================

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS glue_catalog.retail_curated
""")

customer_df.writeTo(
    "glue_catalog.retail_curated.customers"
).using("iceberg").createOrReplace()

product_df.writeTo(
    "glue_catalog.retail_curated.products"
).using("iceberg").createOrReplace()

order_df.writeTo(
    "glue_catalog.retail_curated.orders"
).using("iceberg").createOrReplace()



######
# One joined dataset
#####

sales_df = (
    order_df
    .join(
        customer_df.select(
            "customer_id",
            "first_name",
            "last_name",
            "country",
            "state",
            "loyalty_points"
        ),
        on="customer_id",
        how="inner"
    )
    .join(
        product_df.select(
            "product_id",
            "product_name",
            "category",
            "brand",
            "cost"
        ),
        on="product_id",
        how="inner"
    )
    .withColumn(
        "customer_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )
    .withColumn(
        "profit",
        F.round(
            F.col("total_amount")
            - (F.col("quantity") * F.col("cost")),
            2
        )
    )
)

sales_df.printSchema()
sales_df.show(truncate=False)
print(f"Sales count: {sales_df.count()}")

sales_df.writeTo(
    "glue_catalog.retail_curated.sales"
).using("iceberg").createOrReplace()

spark.sql("SHOW TABLES IN glue_catalog.retail_curated").show(truncate=False)

spark.table("glue_catalog.retail_curated.sales").show(10, truncate=False)