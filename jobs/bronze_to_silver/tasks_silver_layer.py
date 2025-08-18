#!/usr/bin/env python3

import logging
import time
import functools
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, concat_ws, year, month, dayofmonth

# Configure logging for HDFS and Docker environment
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()  # Only console output for Docker containers
    ],
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

# Decorator for logging
def log_execution(function):
    """Decorator to log function execution with timing and record count"""
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting execution of {function.__name__}")
        
        result = function(*args, **kwargs)
        
        # If result is a DataFrame, log record count
        if hasattr(result, 'count'):
            record_count = result.count()
            logger.info(f"Function {function.__name__} processed {record_count} records")
        
        execution_time = time.time() - start_time
        logger.info(f"Function {function.__name__} completed in {execution_time:.2f} seconds")
        
        return result
    
    return wrapper

# Include transformation functions directly in the file to work in cluster mode
@log_execution
def clean_null_dates(df):
    logger.info("Cleaning null dates in DataFrame")
    for col_name in [
        "created_date", 
        "due_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

@log_execution
def clean_null_numbers(df):
    logger.info("Cleaning null numbers in DataFrame")
    for col_name in [
        "estimated_hours", 
        "actual_hours",
        "dependencies"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

@log_execution
def clean_null_strings(df):
    logger.info("Cleaning null strings in DataFrame")
    for col_name in [
        "name", 
        "description", 
        "status", 
        "priority"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df

@log_execution
def clean_arrays(df):
    logger.info("Cleaning arrays in DataFrame")
    return (
        df.withColumn("tags", when(col("tags").isNull(), "No tags").otherwise(col("tags")))
    )

@log_execution
def main_etl_process():
    """Main ETL process with comprehensive logging"""
    logger.info("Starting Tasks Silver Layer ETL Process")
    spark = SparkSession.builder.appName('Tasks-Tech-Company-Application-Cluster').getOrCreate()
    logger.info("Spark session created successfully")

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
    logger.info("Reading JSON file from bronze layer")
    df_tasks = spark.read.schema(tasks_schema).json('hdfs://master:8080/opt/spark/data/bronze_layer/tasks.json', multiLine=True)
    logger.info(f"Initial record count: {df_tasks.count()}")

    # 2. Transform empty strings in null values
    logger.info("Transforming empty strings to null values")
    df_tasks = df_tasks.select([
        when((col(c) == "") | (col(c) == "[]"), None).otherwise(col(c)).alias(c) for c in df_tasks.columns
    ])

    # 3. Add timestamp column
    logger.info("Adding timestamp column")
    df_tasks = df_tasks.withColumn('received_at', current_timestamp())

    # 4. Drop null values (Referential Integrity)
    logger.info("Applying referential integrity constraints")
    initial_count = df_tasks.count()
    df_tasks = df_tasks.filter(col("id").isNotNull())
    df_tasks = df_tasks.filter(col("project_id").isNotNull())
    df_tasks = df_tasks.filter(col("assigned_to").isNotNull())
    final_count = df_tasks.count()
    logger.info(f"Records after referential integrity: {final_count} (dropped {initial_count - final_count} records)")

    # 5. Apply modularized transformations
    logger.info("Applying data cleaning transformations")
    df_tasks = clean_null_dates(df_tasks)
    df_tasks = clean_null_numbers(df_tasks)
    df_tasks = clean_null_strings(df_tasks)
    df_tasks = clean_arrays(df_tasks)

    # 6. Add partitioning columns
    logger.info("Adding partitioning columns")
    df_tasks = df_tasks.withColumn("year_created_date", year(col("created_date")))
    df_tasks = df_tasks.withColumn("month_created_date", month(col("created_date")))
    df_tasks = df_tasks.withColumn("day_created_date", dayofmonth(col("created_date")))

    # 7. Write to parquet with partitioning
    logger.info("Writing data to silver layer parquet format")
    df_tasks.write.partitionBy("year_created_date", "month_created_date", "day_created_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/tasks.parquet", mode="overwrite")
    logger.info("Data successfully written to silver layer")

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Tasks Silver Layer ETL Process completed successfully")

if __name__ == "__main__":
    main_etl_process() 