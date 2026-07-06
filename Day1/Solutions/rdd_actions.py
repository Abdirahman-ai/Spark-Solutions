from pyspark import SparkContext


# ============================================================
# Spark Context
# ============================================================

sc = SparkContext("local[*]", "RDDActions")


# ============================================================
# Task 1: Basic Retrieval Actions
# ============================================================

numbers = sc.parallelize([
    10, 5, 8, 3, 15,
    12, 7, 20, 1, 9
])


# Task A: collect() - Get all elements

all_nums = numbers.collect()

print("\nTask 1A - All Numbers:")
print(all_nums)


# Task B: count() - Count elements

count = numbers.count()

print("\nTask 1B - Count:")
print(count)


# Task C: first() - Get first element

first = numbers.first()

print("\nTask 1C - First Element:")
print(first)


# Task D: take(n) - Get first 3 elements

first_three = numbers.take(3)

print("\nTask 1D - First 3 Elements:")
print(first_three)


# Task E: top(n) - Get largest 3 elements

top_three = numbers.top(3)

print("\nTask 1E - Top 3 Elements:")
print(top_three)


# Task F: takeOrdered(n) - Get smallest 3 elements

smallest_three = numbers.takeOrdered(3)

print("\nTask 1F - Smallest 3 Elements:")
print(smallest_three)


# ============================================================
# Task 2: Aggregation Actions
# ============================================================


# Task A: reduce() - Sum all numbers

total = numbers.reduce(
    lambda x, y: x + y
)

print("\nTask 2A - Sum:")
print(total)


# Task B: reduce() - Find maximum

maximum = numbers.reduce(
    lambda x, y: x if x > y else y
)

print("\nTask 2B - Maximum:")
print(maximum)


# Task C: reduce() - Find minimum

minimum = numbers.reduce(
    lambda x, y: x if x < y else y
)

print("\nTask 2C - Minimum:")
print(minimum)


# Task D: fold() - Sum with zero value

folded_sum = numbers.fold(
    0,
    lambda x, y: x + y
)

print("\nTask 2D - Folded Sum:")
print(folded_sum)


# ============================================================
# Task 3: countByValue()
# ============================================================

colors = sc.parallelize([
    "red",
    "blue",
    "red",
    "green",
    "blue",
    "red",
    "yellow"
])


# Count occurrences of each color

color_counts = colors.countByValue()

print("\nTask 3 - Color Counts:")
print(dict(color_counts))


# Expected:
# {
#     'red': 3,
#     'blue': 2,
#     'green': 1,
#     'yellow': 1
# }


# ============================================================
# Stop Spark Context
# ============================================================

sc.stop()