#!/usr/bin/env python3

import logging
import time
import functools
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import col, when, current_timestamp, year, month, dayofmonth

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()
    ],
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)


def log_execution(function):
    """Decorator to log function execution with timing and record count"""
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting execution of {function.__name__}")
        result = function(*args, **kwargs)
        
        if hasattr(result, 'count'):
            logger.info(f"Function {function.__name__} processed {result.count()} records")
        
        execution_time = time.time() - start_time
        logger.info(f"Function {function.__name__} completed in {execution_time:.2f} seconds")
        return result
    return wrapper


@log_execution
def clean_null_dates(df):
    logger.info("Cleaning null dates in DataFrame")
    for col_name in [
        "contract_start_date", 
        "last_contact_date", 
        "customer_since"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "0000-01-01").otherwise(col(col_name)))
    return df

@log_execution
def clean_null_numbers(df):
    logger.info("Cleaning null numbers in DataFrame")
    for col_name in [
        "contract_value", 
        "annual_revenue", 
        "employee_count"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    return df

@log_execution
def clean_null_floats(df):
    logger.info("Cleaning null floats in DataFrame")
    for col_name in [
        "churn_risk_score", 
        "satisfaction_score", 
        "upsell_potential"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0.0).otherwise(col(col_name)))
    return df

@log_execution
def clean_null_strings(df):
    logger.info("Cleaning null strings in DataFrame")
    for col_name in [
        "company_name", 
        "industry", 
        "contact_person", 
        "email", 
        "phone", 
        "address", 
        "country",
        "status", 
        "payment_history", 
        "contract_type"
    ]:
        df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
    return df

@log_execution
def main_etl_process():
    """Main ETL process with comprehensive logging"""
    logger.info("Starting Clients Silver Layer ETL Process")
    
    # Initialize Spark session
    spark = SparkSession.builder.appName('Clients-Tech-Company-Application-Cluster').getOrCreate()
    logger.info("Spark session created successfully")

    # Schema definition
    clients_schema = StructType([
        StructField("id", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("contact_person", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("country", StringType(), True),
        StructField("contract_start_date", StringType(), True),
        StructField("contract_value", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("account_manager_id", StringType(), True),
        StructField("annual_revenue", IntegerType(), True),
        StructField("employee_count", IntegerType(), True),
        StructField("last_contact_date", StringType(), True),
        StructField("customer_since", StringType(), True),
        StructField("churn_risk_score", FloatType(), True),
        StructField("satisfaction_score", FloatType(), True),
        StructField("payment_history", StringType(), True),
        StructField("contract_type", StringType(), True),
        StructField("upsell_potential", FloatType(), True)
    ])

    # 1. Read CSV file with header
    logger.info("Reading CSV file from bronze layer")
    df_clients = spark.read.schema(clients_schema).csv('hdfs://master:8080/opt/spark/data/bronze_layer/clients.csv', header=True)
    logger.info(f"Initial record count: {df_clients.count()}")

    # 2. Transform empty strings in null values
    logger.info("Transforming empty strings to null values")
    df_clients = df_clients.select([
        when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_clients.columns
    ])

    # 3. Add timestamp column
    logger.info("Adding timestamp column")
    df_clients = df_clients.withColumn('received_at', current_timestamp())

    # 4. Drop null values (Referential Integrity)
    logger.info("Applying referential integrity constraints")
    initial_count = df_clients.count()
    df_clients = df_clients.filter(col("id").isNotNull())
    df_clients = df_clients.filter(col("account_manager_id").isNotNull())
    final_count = df_clients.count()
    logger.info(f"Records after referential integrity: {final_count} (dropped {initial_count - final_count} records)")

    # 5. Apply modularized transformations
    logger.info("Applying data cleaning transformations")
    df_clients = clean_null_dates(df_clients)
    df_clients = clean_null_numbers(df_clients)
    df_clients = clean_null_floats(df_clients)
    df_clients = clean_null_strings(df_clients)

    # 6. Add partitioning columns
    logger.info("Adding partitioning columns")
    df_clients = df_clients.withColumn("year_contract_start", year(col("contract_start_date")))
    df_clients = df_clients.withColumn("month_contract_start", month(col("contract_start_date")))
    df_clients = df_clients.withColumn("day_contract_start", dayofmonth(col("contract_start_date")))

    # 7. Write to parquet with partitioning
    logger.info("Writing data to silver layer parquet format")
    df_clients.write.partitionBy("year_contract_start", "month_contract_start", "day_contract_start").parquet("hdfs://master:8080/opt/spark/data/silver_layer/clients.parquet", mode="overwrite")
    logger.info("Data successfully written to silver layer")

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Clients Silver Layer ETL Process completed successfully")

if __name__ == "__main__":
    main_etl_process() 