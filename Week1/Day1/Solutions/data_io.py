### Task 1: Load Data with textFile
from pyspark import SparkContext

sc = SparkContext("local[*]", "DataIO")

# Load the CSV file
raw_txt_rdd = sc.textFile("../Solutions/sales_data.csv")

# Skip header line
header = raw_txt_rdd.first()
# print(header)
data_rdd = raw_txt_rdd.filter(lambda row: row != header)

print("header: ", header)
print(f"Data records: {data_rdd.count()}")
print(f"First record {data_rdd.first()}")


### Task 2: Parse CSV Records
def parse_record(line):
    parts = line.split(",")
    
    return {
        "Product_id": parts[0],
        "Name":parts[1],
        "Category:": parts[2],
        "Price": float(parts[3]),
        "Quantity": int(parts[4])
    }

parsed = data_rdd.map(parse_record)
# print(parsed.collect())

for record in parsed.collect():
    print(record)


### Task 3: Process and Save Results
revenue = parsed.map(
    lambda r: f"{r['Product_id']}, {r['Name']}, {r['Price'] * r['Quantity']:.2f}")
# print(revenue.collect())

# Calculate revenue for each product
# Save to output directory
# YOUR CODE: Use saveAsTextFile to save revenue data
output_path = "./output/revenue"
revenue.saveAsTextFile(output_path)

### Task 4: Load Multiple Files
 
# Create additional test files and load with wildcards:
# YOUR CODE: Create sales_data_2.csv with more records
# YOUR CODE: Load all CSV files using wildcard pattern
# all_data = sc.textFile("sales_data*.csv")

all_files = sc.textFile("../Solutions/sales_data*.csv")
print("First 5 rows: ", all_files.take(5))

### Task 5: Coalesce Output

# Save to a single output file:

# YOUR CODE: Use coalesce(1) before saveAsTextFile
# This creates a single output file instead of multiple parts

coalesce_output_path = "./output/revenue_single"

coalesce_data = revenue.coalesce(1)
coalesce_data.saveAsTextFile(coalesce_output_path)

sc.stop()
