## Assignment 2: Day 2

## EXAMPLE CODE: FOR THE TRANSFORMATION.ppx
### Narrow vs. Wide 

# Narrow — stays in place
df.filter(df.age > 21)
df.select("name", "age")
df.withColumn("bonus", df.salary * 0.1)

# Wide — forces a shuffle
df.groupBy("department").count()
df.join(other_df, "id")
df.orderBy("salary")


###  Shuffling (how to actually see a shuffle happen)
df.groupBy("department").count().explain()
# Look for "Exchange" in the output — that's the shuffle step

# Check how many shuffle partitions Spark uses by default
spark.conf.get("spark.sql.shuffle.partitions")   # usually 200
spark.conf.set("spark.sql.shuffle.partitions", 50)

# repartition() (a couple more variations beyond the count-only version)
df.repartition(8)                    # 8 partitions, round-robin
df.repartition("department")         # partition by column (hash)
df.repartition(8, "department")      # both: 8 partitions, hashed by column

# Check partition count before/after
df.rdd.getNumPartitions()

### coalesce()
df.coalesce(1)     # merge down to a single output file — common before .write()
df.coalesce(4)

# A common real pattern:
df.filter(df.year == 2024).coalesce(4).write.parquet("path/")

### partitionBy()

# Single column
df.write.partitionBy("year").parquet("path/")

# Multiple columns — nested folders: year=2024/month=03/
df.write.partitionBy("year", "month").parquet("path/")

# Reading back — Spark automatically prunes folders
spark.read.parquet("path/").filter("year = 2024")


# Bucketing

df.write \
  .bucketBy(8, "user_id") \
  .sortBy("user_id") \
  .saveAsTable("users_bucketed")

# Joining two tables bucketed the same way on user_id skips the shuffle
spark.sql("""
  SELECT * FROM users_bucketed a
  JOIN orders_bucketed b ON a.user_id = b.user_id
""")

