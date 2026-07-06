from pyspark import SparkConf, SparkContext
from prettytable import PrettyTable

# ============================================================
# Spark Configuration
# ============================================================

conf = (
    SparkConf()
    .setMaster("local[*]")
    .setAppName("CustomerOrders")
)

sc = SparkContext(conf=conf)

# ============================================================
# Load Customer Order Data
# ============================================================

customer_orders = sc.textFile("../Day3/customer-orders.csv")

# ============================================================
# Calculate Total Amount Spent by Each Customer
# ============================================================

customer_totals = (
    customer_orders
    .map(lambda line: line.split(","))
    .map(lambda fields: (int(fields[0]), float(fields[2])))
    .reduceByKey(lambda amount1, amount2: amount1 + amount2)
    .sortBy(lambda customer: customer[1], ascending=False)
)

# ============================================================
# Collect and Display Results
# ============================================================

results = customer_totals.collect()

table = PrettyTable()
table.field_names = ["Customer ID", "Total Spent"]

for customer_id, total_spent in results:
    table.add_row([customer_id, f"${total_spent:.2f}"])

print(table)

# ============================================================
# Stop Spark Context
# ============================================================

sc.stop()
