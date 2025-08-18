#!/usr/bin/env python3

import logging
import time
import functools
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, LongType
)
from pyspark.sql.functions import col, when, current_timestamp, year, month, dayofmonth

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
    return (
        df.withColumn("effective_date", when(col("effective_date").isNull(), "0000-01-01").otherwise(col("effective_date")))
    )

@log_execution
def clean_null_numbers(df):
    logger.info("Cleaning null numbers in DataFrame")
    for col_name in [
        "salary", 
        "bonus_amount", 
        "stock_options"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

@log_execution
def clean_null_strings(df):
    logger.info("Cleaning null strings in DataFrame")
    for col_name in [
        "change_reason", 
        "position", 
        "currency"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df

@log_execution
def main_etl_process():
    """Main ETL process with comprehensive logging"""
    logger.info("Starting Salary History Silver Layer ETL Process")
    spark = SparkSession.builder.appName('Salary-History-Tech-Company-Application-Cluster').getOrCreate()
    logger.info("Spark session created successfully")

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
    logger.info("Reading Parquet file from bronze layer")
    df_salary_history = spark.read.schema(salary_history_schema).parquet("hdfs://master:8080/opt/spark/data/bronze_layer/salary_history.parquet")
    logger.info(f"Initial record count: {df_salary_history.count()}")

    # 2. Transform empty strings in null values
    logger.info("Transforming empty strings to null values")
    df_salary_history = df_salary_history.select([
        when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_salary_history.columns
    ])

    # 3. Add timestamp column
    logger.info("Adding timestamp column")
    df_salary_history = df_salary_history.withColumn('received_at', current_timestamp())

    # 4. Drop null values (Referential Integrity)
    logger.info("Applying referential integrity constraints")
    initial_count = df_salary_history.count()
    df_salary_history = df_salary_history.filter(col("id").isNotNull())
    df_salary_history = df_salary_history.filter(col("employee_id").isNotNull())
    df_salary_history = df_salary_history.filter(col("department_id").isNotNull())
    final_count = df_salary_history.count()
    logger.info(f"Records after referential integrity: {final_count} (dropped {initial_count - final_count} records)")

    # 5. Apply modularized transformations
    logger.info("Applying data cleaning transformations")
    df_salary_history = clean_null_dates(df_salary_history)
    df_salary_history = clean_null_numbers(df_salary_history)
    df_salary_history = clean_null_strings(df_salary_history)

    # 6. Add partitioning columns
    logger.info("Adding partitioning columns")
    df_salary_history = df_salary_history.withColumn("year_effective_date", year(col("effective_date")))
    df_salary_history = df_salary_history.withColumn("month_effective_date", month(col("effective_date")))
    df_salary_history = df_salary_history.withColumn("day_effective_date", dayofmonth(col("effective_date")))

    # 7. Write to parquet with partitioning
    logger.info("Writing data to silver layer parquet format")
    df_salary_history.write.partitionBy("year_effective_date", "month_effective_date", "day_effective_date").parquet("hdfs://master:8080/opt/spark/data/silver_layer/salary_history.parquet", mode="overwrite")
    logger.info("Data successfully written to silver layer")

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Salary History Silver Layer ETL Process completed successfully")

if __name__ == "__main__":
    main_etl_process() 