from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, year, month, dayofmonth
from transformations.departments_transformations import clean_arrays, clean_null_floats, clean_null_dates, clean_null_numbers, clean_null_strings

spark = SparkSession.builder.appName('Departments-Tech-Company-Application').getOrCreate()

# 0. Schema definition
departments_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("budget", FloatType(), True),
    StructField("location", StringType(), True),
    StructField("manager_id", StringType(), True),
    StructField("headcount", IntegerType(), True),
    StructField("founded_date", StringType(), True),
    StructField("description", StringType(), True),
    StructField("tech_stack", StringType(), True),
    StructField("quarterly_budget", FloatType(), True),
    StructField("yearly_revenue", FloatType(), True),
    StructField("cost_center", StringType(), True),
    StructField("region", StringType(), True),
    StructField("office_size", IntegerType(), True),
    StructField("last_audit_date", StringType(), True),
])

# 1. Read CSV file with header
df_departments = spark.read.schema(departments_schema).csv('hdfs:///opt/spark/data/bronze_layer/departments.csv', header=True)

# 2. Transform empty strings in null values
df_departments = df_departments.select([
    when((col(c) == "") | (col(c) == "[]"), None).otherwise(col(c)).alias(c) for c in df_departments.columns
])

# 3. Add timestamp column
df_departments = df_departments.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_departments = df_departments.filter(col("id").isNotNull())
df_departments = df_departments.filter(col("manager_id").isNotNull())

# 5. Apply modularized transformations
df_departments = clean_null_dates(df_departments)
df_departments = clean_null_floats(df_departments)
df_departments = clean_null_numbers(df_departments)
df_departments = clean_null_strings(df_departments)
df_departments = clean_arrays(df_departments)

# 6. Add partitioning columns
df_departments = df_departments.withColumn("year_founded_date", year(col("founded_date")))
df_departments = df_departments.withColumn("month_founded_date", month(col("founded_date")))
df_departments = df_departments.withColumn("day_founded_date", dayofmonth(col("founded_date")))

# 7. Write to parquet with partitioning
df_departments.write.partitionBy("year_founded_date", "month_founded_date", "day_founded_date").parquet("hdfs:///opt/spark/data/silver_layer/departments.parquet", mode="overwrite")