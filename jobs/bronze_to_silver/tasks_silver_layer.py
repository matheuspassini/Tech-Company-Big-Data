#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, concat_ws, year, month, dayofmonth

# Include transformation functions directly in the file to work in cluster mode
def clean_null_dates(df):
    for col_name in [
        "created_date", 
        "due_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

def clean_null_numbers(df):
    for col_name in [
        "estimated_hours", 
        "actual_hours",
        "dependencies"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

def clean_null_strings(df):
    for col_name in [
        "name", 
        "description", 
        "status", 
        "priority"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df

def clean_arrays(df):
    return (
        df.withColumn("tags", when(col("tags").isNull(), "No tags").otherwise(col("tags")))
    )

spark = SparkSession.builder.appName('Tasks-Tech-Company-Application-Cluster').getOrCreate()

# Schema definition
tasks_schema = StructType([
    StructField("id", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("name", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("assigned_to", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("due_date", StringType(), True),
    StructField("estimated_hours", IntegerType(), True),
    StructField("actual_hours", IntegerType(), True),
    StructField("dependencies", StringType(), True),
    StructField("tags", StringType(), True)
])

# 1. Read JSON file with header
df_tasks = spark.read.schema(tasks_schema).json('hdfs://master:8080/opt/spark/data/bronze_layer/tasks.json', multiLine=True)

# 2. Transform empty strings in null values
df_tasks = df_tasks.select([
    when((col(c) == "") | (col(c) == "[]"), None).otherwise(col(c)).alias(c) for c in df_tasks.columns
])

# 3. Add timestamp column
df_tasks = df_tasks.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_tasks = df_tasks.filter(col("id").isNotNull())
df_tasks = df_tasks.filter(col("project_id").isNotNull())
df_tasks = df_tasks.filter(col("assigned_to").isNotNull())

# 5. Apply modularized transformations
df_tasks = clean_null_dates(df_tasks)
df_tasks = clean_null_numbers(df_tasks)
df_tasks = clean_null_strings(df_tasks)
df_tasks = clean_arrays(df_tasks)

# 6. Add partitioning columns
df_tasks = df_tasks.withColumn("year_created_date", year(col("created_date")))
df_tasks = df_tasks.withColumn("month_created_date", month(col("created_date")))
df_tasks = df_tasks.withColumn("day_created_date", dayofmonth(col("created_date")))

# 7. Write to parquet with partitioning
df_tasks.write.partitionBy("year_created_date", "month_created_date", "day_created_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/tasks.parquet", mode="overwrite")

spark.stop() 