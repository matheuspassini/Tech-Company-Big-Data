#!/usr/bin/env python3

import logging
import time
import functools
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, year, month, dayofmonth

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
        "founded_date",
        "last_audit_date"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

@log_execution
def clean_null_floats(df):
    logger.info("Cleaning null floats in DataFrame")
    for col_name in [
        "budget",
        "quarterly_budget",
        "yearly_revenue"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
    return df    

@log_execution
def clean_null_numbers(df):
    logger.info("Cleaning null numbers in DataFrame")
    for col_name in [
        "headcount",
        "office_size"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

@log_execution
def clean_null_strings(df):
    logger.info("Cleaning null strings in DataFrame")
    for col_name in [
        "location",
        "region",
        "description"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df
    
@log_execution
def clean_arrays(df):
    logger.info("Cleaning arrays in DataFrame")
    return (
        df.withColumn("tech_stack", when(col("tech_stack").isNull(), "Not available").otherwise(col("tech_stack")))
    )

@log_execution
def main_etl_process():
    """Main ETL process with comprehensive logging"""
    logger.info("Starting Departments Silver Layer ETL Process")
    spark = SparkSession.builder.appName('Departments-Tech-Company-Application-Cluster').getOrCreate()
    logger.info("Spark session created successfully")

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
    logger.info("Reading CSV file from bronze layer")
    df_departments = spark.read.schema(departments_schema).csv('hdfs://master:8080/opt/spark/data/bronze_layer/departments.csv', header=True)
    logger.info(f"Initial record count: {df_departments.count()}")

    # 2. Transform empty strings in null values
    logger.info("Transforming empty strings to null values")
    df_departments = df_departments.select([
        when((col(c) == "") | (col(c) == "[]"), None).otherwise(col(c)).alias(c) for c in df_departments.columns
    ])

    # 3. Add timestamp column
    logger.info("Adding timestamp column")
    df_departments = df_departments.withColumn('received_at', current_timestamp())

    # 4. Drop null values (Referential Integrity)
    logger.info("Applying referential integrity constraints")
    initial_count = df_departments.count()
    df_departments = df_departments.filter(col("id").isNotNull())
    df_departments = df_departments.filter(col("manager_id").isNotNull())
    final_count = df_departments.count()
    logger.info(f"Records after referential integrity: {final_count} (dropped {initial_count - final_count} records)")

    # 5. Apply modularized transformations
    logger.info("Applying data cleaning transformations")
    df_departments = clean_null_dates(df_departments)
    df_departments = clean_null_floats(df_departments)
    df_departments = clean_null_numbers(df_departments)
    df_departments = clean_null_strings(df_departments)
    df_departments = clean_arrays(df_departments)

    # 6. Add partitioning columns
    logger.info("Adding partitioning columns")
    df_departments = df_departments.withColumn("year_founded_date", year(col("founded_date")))
    df_departments = df_departments.withColumn("month_founded_date", month(col("founded_date")))
    df_departments = df_departments.withColumn("day_founded_date", dayofmonth(col("founded_date")))

    # 7. Write to parquet with partitioning
    logger.info("Writing data to silver layer parquet format")
    df_departments.write.partitionBy("year_founded_date", "month_founded_date", "day_founded_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/departments.parquet", mode="overwrite")
    logger.info("Data successfully written to silver layer")

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Departments Silver Layer ETL Process completed successfully")

if __name__ == "__main__":
    main_etl_process() 