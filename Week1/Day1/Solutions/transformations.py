from pyspark import SparkContext

# ============================================================
# Spark Context
# ============================================================

sc = SparkContext("local[*]", "Transformations")


# ============================================================
# Task 1: Narrow Transformations
# ============================================================

logs = sc.parallelize([
    "2024-01-15 10:00:00 INFO User login: alice",
    "2024-01-15 10:01:00 ERROR Database connection failed",
    "2024-01-15 10:02:00 INFO User login: bob",
    "2024-01-15 10:03:00 WARN Memory usage high",
    "2024-01-15 10:04:00 ERROR Timeout occurred",
    "2024-01-15 10:05:00 INFO Data processed: 1000 records",
    "2024-01-15 10:06:00 DEBUG Cache hit rate: 95%"
])


# Task A: Filter only ERROR logs

errors = logs.filter(
    lambda line: "ERROR" in line
)

print("\nTask 1A - ERROR Logs:")
print(errors.collect())


# Task B: Extract the log level from each line

levels = logs.map(
    lambda line: line.split()[2]
)

print("\nTask 1B - Log Levels:")
print(levels.collect())


# Task C: Get messages from ERROR logs only

error_messages = (
    logs
    .filter(lambda line: "ERROR" in line)
    .map(lambda line: " ".join(line.split()[3:]))
)

print("\nTask 1C - ERROR Messages:")
print(error_messages.collect())


# ============================================================
# Task 2: Wide Transformations
# ============================================================

words = sc.parallelize([
    "spark",
    "hadoop",
    "spark",
    "data",
    "hadoop",
    "spark",
    "python",
    "data",
    "spark",
    "scala"
])


# Task A: Get unique words

unique = words.distinct()

print("\nTask 2A - Unique Words:")
print(unique.collect())


# Task B: Count occurrences of each word

word_counts = (
    words
    .map(lambda word: (word, 1))
    .reduceByKey(lambda x, y: x + y)
)

print("\nTask 2B - Word Counts:")
print(word_counts.collect())


# Task C: Sort word counts by count descending

sorted_counts = word_counts.sortBy(
    lambda item: item[1],
    ascending=False
)

print("\nTask 2C - Sorted Word Counts:")
print(sorted_counts.collect())


# ============================================================
# Task 3: Set Operations
# ============================================================

set1 = sc.parallelize([1, 2, 3, 4, 5])
set2 = sc.parallelize([4, 5, 6, 7, 8])


# Task A: Union

combined = set1.union(set2)

print("\nTask 3A - Union:")
print(combined.collect())


# Task B: Intersection

common = set1.intersection(set2)

print("\nTask 3B - Intersection:")
print(common.collect())


# Task C: Subtract

difference = set1.subtract(set2)

print("\nTask 3C - Difference:")
print(difference.collect())


# ============================================================
# Task 4: Practical Log Analysis Pipeline
# ============================================================

web_logs = sc.parallelize([
    "192.168.1.1 GET /home 200 150ms",
    "192.168.1.2 GET /products 200 230ms",
    "192.168.1.1 POST /login 200 180ms",
    "192.168.1.3 GET /home 404 50ms",
    "192.168.1.2 GET /products 200 210ms",
    "192.168.1.1 GET /home 200 120ms",
    "192.168.1.4 GET /admin 403 30ms"
])


# Pipeline:
# 1. Filter successful requests with status 200
# 2. Extract the endpoint
# 3. Convert endpoints to key-value pairs
# 4. Count requests per endpoint
# 5. Sort by count descending

endpoint_counts = (
    web_logs
    .filter(lambda line: line.split()[3] == "200")
    .map(lambda line: line.split()[2])
    .map(lambda endpoint: (endpoint, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda item: item[1], ascending=False)
)

print("\nTask 4 - Endpoint Request Counts:")
print(endpoint_counts.collect())


# ============================================================
# Stop Spark Context
# ============================================================

sc.stop()