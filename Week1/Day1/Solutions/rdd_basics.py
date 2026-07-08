from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")


# ============================================================
# Task 1: Create RDDs from Different Sources
# ============================================================

# 1. Create RDD from a Python list
numbers = sc.parallelize([
    1, 2, 3, 4, 5,
    6, 7, 8, 9, 10
])

print("Task 1")
print(f"Numbers: {numbers.collect()}")
print(f"Original partitions: {numbers.getNumPartitions()}")


# 2. Create the same RDD with exactly 4 partitions
four_partitions = sc.parallelize(
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    4
)

print(f"Four partitions: {four_partitions.getNumPartitions()}")


# 3. Create RDD from range(1, 101)
range_rdd = sc.parallelize(range(1, 101))

print(f"Range RDD: {range_rdd.collect()}")


# ============================================================
# Task 2: Apply map() Transformation
# ============================================================

print("\nTask 2")


# Task A: Square each number

squared = numbers.map(
    lambda x: x ** 2
)

print(f"Squared: {squared.collect()}")


# Task B: Convert numbers to strings with prefix

prefixed = numbers.map(
    lambda x: f"num_{x}"
)

print(f"Prefixed: {prefixed.collect()}")


# ============================================================
# Task 3: Apply filter() Transformation
# ============================================================

print("\nTask 3")


# Task A: Keep only even numbers

evens = numbers.filter(
    lambda x: x % 2 == 0
)

print(f"Even numbers: {evens.collect()}")


# Task B: Keep numbers greater than 5

greater_than_5 = numbers.filter(
    lambda x: x > 5
)

print(f"Greater than 5: {greater_than_5.collect()}")


# Task C: Keep numbers that are even AND greater than 5

combined = numbers.filter(
    lambda x: x > 5 and x % 2 == 0
)

print(f"Even and greater than 5: {combined.collect()}")


# ============================================================
# Task 4: Apply flatMap() Transformation
# ============================================================

print("\nTask 4")


sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])


# Task A: Split sentences into individual words

words = sentences.flatMap(
    lambda line: line.split()
)

print(f"Words: {words.collect()}")


# Task B: Create pairs of (word, length)

word_lengths = words.map(
    lambda word: (word, len(word))
)

print(f"Word lengths: {word_lengths.collect()}")


# ============================================================
# Task 5: Chain Transformations
# ============================================================

print("\nTask 5")


logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])


# Pipeline:
# 1. Keep only ERROR lines
# 2. Split ERROR lines into words
# 3. Convert each word to uppercase

error_words = (
    logs
    .filter(lambda line: line.startswith("ERROR"))
    .flatMap(lambda line: line.split())
    .map(lambda word: word.upper())
)

print(f"Error words: {error_words.collect()}")


# Stop SparkContext
sc.stop()