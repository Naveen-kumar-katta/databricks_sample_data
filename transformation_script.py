# Importing required packages
from pyspark.sql import SparkSession  # Entry point to create Spark DataFrame and configure Spark settings
import pyspark.sql.functions as F      # Used for DataFrame transformations
import pyspark.sql.types as T          # Used for defining DataFrame schemas and data types
from datetime import datetime          # Used for working with date and time


# Step 1: Initializing Spark session
spark = SparkSession.builder.appName('Employee_ETL_Demo').getOrCreate()

# Step 2: Defining file paths for employee CSV chunks and department JSONL file
department_file_path = 's3a://aws-databricks-practice/emp_dept_sample_data/department_json_data/departments.jsonl'

# The asterisk (*) acts as a wildcard to match all CSV files in the folder (e.g., chunk1.csv, chunk2.csv, etc.).
employee_file_path = "s3a://aws-databricks-practice/emp_dept_sample_data/employees_csv_chunks/*.csv"


# Step 3: Defining custom schemas (optional if inferSchema is used)
csv_schema = T.StructType([
    T.StructField("employee_id", T.IntegerType(), True),  # Define a field named 'employee_id' with Integer type, allowing null values
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("mobile_no", T.StringType(), True),
    T.StructField("department_id", T.IntegerType(), True),
    T.StructField("salary", T.DoubleType(), True)
])
json_schema = T.StructType([
    T.StructField("department_id", T.IntegerType(), True),
    T.StructField("department_name", T.StringType(), True)
])

# Reading employee CSV file using different approaches
df_employees = spark.read.csv(employee_file_path, header=True, schema=csv_schema)

# Reading department JSONL file using defined schema
df_dept = spark.read.schema(json_schema).json(department_file_path)

# Step 4: Performing transformations on employee data

# Filter out records with null email
df_emp_filtered = df_employees.filter(df_employees.email.isNotNull())

# Add a full_name column by concatenating first and last names
df_emp_filtered = df_emp_filtered.withColumn(
    "full_name", 
    F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
)

# Mask mobile number (show first 4 digits and mask the rest)
df_emp_filtered = df_emp_filtered.withColumn(
    "mobile_no", 
    F.concat(df_emp_filtered.mobile_no.substr(1, 4), F.lit("******"))
)

# Step 5: Joining employee data with department info on department_id
df_joined = df_emp_filtered.join(df_dept, on='department_id', how='inner')

# Step 6: Writing the final output as a single CSV file into AWS S3 partitioned by current year/month/day
cur_date = datetime.now()
year, month, day = cur_date.year, cur_date.month, cur_date.day
# Define the S3 path for storing the output
output_path = f"s3a://aws-databricks-practice/processed_data/{year}/{month}/{day}/"

# Write the transformed data to S3 as a single CSV file
df_joined.coalesce(1).write.option("header", "true").csv(output_path)
