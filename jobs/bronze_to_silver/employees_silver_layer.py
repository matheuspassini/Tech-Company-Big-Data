from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp
from transformations.employees_transformations import clean_null_dates, clean_null_numbers, clean_null_strings, clean_null_booleans, clean_arrays

spark = SparkSession.builder.appName('Tech-Company-Application').getOrCreate()

# Schema definition
employee_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("position", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("performance_score", FloatType(), True),
    StructField("skills", StringType(), True),
    StructField("education", StringType(), True),
    StructField("years_experience", IntegerType(), True),
    StructField("certifications", StringType(), True),
    StructField("projects_assigned", IntegerType(), True),
    StructField("is_manager", BooleanType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("emergency_contact", StringType(), True),
    StructField("emergency_phone", StringType(), True),
    StructField("work_location", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("last_review_date", StringType(), True),
    StructField("next_review_date", StringType(), True),
    StructField("attendance_rate", FloatType(), True),
    StructField("overtime_hours", IntegerType(), True),
    StructField("training_hours", IntegerType(), True),
    StructField("sick_days", IntegerType(), True),
    StructField("vacation_days_used", IntegerType(), True),
    StructField("vacation_days_remaining", IntegerType(), True),
])

# 1. Read JSON file with header
df_employee = spark.read.schema(employee_schema).json('hdfs:///opt/spark/data/bronze_layer/employees.json', multiLine=True)

# 1. Transform empty strings in null values
df_employee = df_employee.select([
    when((col(c) == "") | (col(c) == "[]"), None).otherwise(col(c)).alias(c) for c in df_employee.columns
])

# 2. Drop columns
cols_to_drop = ['emergency_contact', 'email', 'name', "phone", "address", "emergency_phone", 'sick_days', 'attendance_rate', 'training_hours']
df_employee = df_employee.drop(*cols_to_drop)

# 3. Add timestamp column
df_employee = df_employee.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_employee = df_employee.filter(col("id").isNotNull())
df_employee = df_employee.filter(col("department_id").isNotNull())
df_employee = df_employee.filter(col("employment_type").isNotNull())
df_employee = df_employee.filter(col("education").isNotNull())
df_employee = df_employee.filter(col("hire_date").isNotNull())

# 5. Apply modularized transformations
df_employee = clean_null_dates(df_employee)
df_employee = clean_null_numbers(df_employee)
df_employee = clean_null_strings(df_employee)
df_employee = clean_null_booleans(df_employee)
df_employee = clean_arrays(df_employee)


# 6. Write to parquet
df_employee.write.parquet("hdfs:///opt/spark/data/silver_layer/employee.parquet", mode="overwrite")