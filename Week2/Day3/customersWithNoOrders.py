from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomersNoOrder").getOrCreate()

customers_df  = spark.read.csv("customers.csv", header=True, inferSchema=True);
customers_df.createOrReplaceTempView("customers")

orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
orders_df.createOrReplaceTempView("Orders")

joined_df = spark.sql("""
    SELECT c.customer_id, c.first_name, c.last_name
    FROM customers c
    LEFT JOIN orders o on c.customer_id = o.customer_id
    WHERE o.order_id is NULL;
""")

customers_df.show(5)
orders_df.show(5)

joined_df.show()

