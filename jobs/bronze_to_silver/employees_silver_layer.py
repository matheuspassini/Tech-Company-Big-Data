#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, year, month, dayofmonth

# Include transformation functions directly in the file to work in cluster mode
def clean_null_dates(df):
    for col_name in [
        "hire_date", 
        "last_review_date", 
        "next_review_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df
    
def clean_null_numbers(df):
    for col_name in [
        "projects_assigned", 
        "overtime_hours", 
        "vacation_days_used", 
        "vacation_days_remaining", 
        "years_experience"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df
    
def clean_null_strings(df):
    for col_name in [
        "position", 
        "work_location"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df
    
def clean_null_booleans(df):
    return (
        df.withColumn("is_manager", when(col("is_manager").isNull(), False).otherwise(col("is_manager")))
    )

def clean_arrays(df):
    return (
        df.withColumn("skills", when(col("skills").isNull(), "No skills").otherwise(col("skills")))
          .withColumn("certifications", when(col("certifications").isNull(), "No certifications").otherwise(col("certifications")))
    )

spark = SparkSession.builder.appName('Employees-Tech-Company-Application-Cluster').getOrCreate()

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

# 0. Read JSON file with header
df_employee = spark.read.schema(employee_schema).json('hdfs://master:8080/opt/spark/data/bronze_layer/employees.json', multiLine=True)

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

# 6. Add partitioning columns
df_employee = df_employee.withColumn("year_hire_date", year(col("hire_date")))
df_employee = df_employee.withColumn("month_hire_date", month(col("hire_date")))
df_employee = df_employee.withColumn("day_hire_date", dayofmonth(col("hire_date")))

# 7. Write to parquet with partitioning
df_employee.write.partitionBy("year_hire_date", "month_hire_date", "day_hire_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/employee.parquet", mode="overwrite")

spark.stop() 