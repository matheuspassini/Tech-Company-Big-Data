#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, year, month, dayofmonth

# Include transformation functions directly in the file to work in cluster mode
def clean_null_dates(df):
    for col_name in [
        "founded_date",
        "last_audit_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

def clean_null_floats(df):
    for col_name in [
        "budget",
        "quarterly_budget",
        "yearly_revenue"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
    return df    
    
def clean_null_numbers(df):
    for col_name in [
        "headcount",
        "office_size"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_strings(df):
    for col_name in [
        "location",
        "region",
        "description"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df
    
def clean_arrays(df):
    return (
        df.withColumn("tech_stack", when(col("tech_stack").isNull(), "Not available").otherwise(col("tech_stack")))
    )

spark = SparkSession.builder.appName('Departments-Tech-Company-Application-Cluster').getOrCreate()

# Schema definition
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
df_departments = spark.read.schema(departments_schema).csv('hdfs://master:8080/opt/spark/data/bronze_layer/departments.csv', header=True)

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
df_departments.write.partitionBy("year_founded_date", "month_founded_date", "day_founded_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/departments.parquet", mode="overwrite")

spark.stop() 