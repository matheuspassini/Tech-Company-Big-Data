#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, LongType
)
from pyspark.sql.functions import col, when, current_timestamp, year, month, dayofmonth

# Include transformation functions directly in the file to work in cluster mode
def clean_null_dates(df):
    return (
        df.withColumn("effective_date", when(col("effective_date").isNull(), "0000-01-01").otherwise(col("effective_date")))
    )

def clean_null_numbers(df):
    for col_name in [
        "salary", 
        "bonus_amount", 
        "stock_options"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_strings(df):
    for col_name in [
        "change_reason", 
        "position", 
        "currency"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df

spark = SparkSession.builder.appName('Salary-History-Tech-Company-Application-Cluster').getOrCreate()

# Schema definition
salary_history_schema = StructType([
    StructField("id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("salary", LongType(), True),
    StructField("effective_date", DateType(), True),
    StructField("change_reason", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("position", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("bonus_amount", LongType(), True),
    StructField("stock_options", LongType(), True)
])

# 1. Read Parquet file
df_salary_history = spark.read.schema(salary_history_schema).parquet("hdfs://master:8080/opt/spark/data/bronze_layer/salary_history.parquet")

# 2. Transform empty strings in null values
df_salary_history = df_salary_history.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_salary_history.columns
])

# 3. Add timestamp column
df_salary_history = df_salary_history.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_salary_history = df_salary_history.filter(col("id").isNotNull())
df_salary_history = df_salary_history.filter(col("employee_id").isNotNull())
df_salary_history = df_salary_history.filter(col("department_id").isNotNull())

# 5. Apply modularized transformations
df_salary_history = clean_null_dates(df_salary_history)
df_salary_history = clean_null_numbers(df_salary_history)
df_salary_history = clean_null_strings(df_salary_history)

# 6. Add partitioning columns
df_salary_history = df_salary_history.withColumn("year_effective_date", year(col("effective_date")))
df_salary_history = df_salary_history.withColumn("month_effective_date", month(col("effective_date")))
df_salary_history = df_salary_history.withColumn("day_effective_date", dayofmonth(col("effective_date")))

# 7. Write to parquet with partitioning
df_salary_history.write.partitionBy("year_effective_date", "month_effective_date", "day_effective_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/salary_history.parquet", mode="overwrite")

spark.stop() 