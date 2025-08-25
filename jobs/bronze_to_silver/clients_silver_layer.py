#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, read_file,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings
)
from utils.config import SPARK_CONFIGS, INPUT_PATHS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY

logger = setup_logging(__name__)

# Schema definition for clients
CLIENTS_SCHEMA = StructType([
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

@log_execution
def add_partitioning_columns(df):
    """Add partitioning columns for optimized storage (job-specific)"""
    logger.info("Adding partitioning columns for optimized storage")
    df = df.withColumn("year_contract_start", year(col("contract_start_date")))
    df = df.withColumn("month_contract_start", month(col("contract_start_date")))
    df = df.withColumn("day_contract_start", dayofmonth(col("contract_start_date")))
    return df

@log_execution
def clients_etl():
    """Main ETL process using shared utilities - MUCH SIMPLER NOW!"""
    logger.info("Starting Clients Silver Layer ETL Process")
    logger.info("Using shared utilities for all operations")

    spark = create_spark_session(SPARK_CONFIGS["clients"])
    input_path = INPUT_PATHS["clients"]
    logger.info(f"Reading clients data from: {input_path}")
    
    # Read CSV with schema and header
    df = spark.read.schema(CLIENTS_SCHEMA).csv(input_path, header=True)
    df = df.cache()
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")

    # Apply transformations using shared utilities
    df = transform_empty_strings(df)  # Shared utility
    df = add_timestamp_column(df)     # Shared utility
    df = apply_referential_integrity(df, REFERENTIAL_INTEGRITY["clients"])  # Shared utility
    df = clean_dataframe(df, CLEANING_RULES["clients"])  # Shared utility
    df = add_partitioning_columns(df)  # Job-specific

    output_path = OUTPUT_PATHS["clients"]
    logger.info(f"Writing cleaned data to silver layer: {output_path}")
    df.write.partitionBy("year_contract_start", "month_contract_start", "day_contract_start").parquet(
        output_path, mode="overwrite"
    )
    final_count = df.count()
    logger.info(f"Successfully processed {final_count} records")
    logger.info("Clients ETL process completed successfully")
    return df

if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("CLIENTS SILVER LAYER ETL JOB")
        logger.info("=" * 80)
        
        # Execute the ETL process
        result_df = clients_etl()
        
        logger.info("=" * 80)
        logger.info("JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        logger.error("Stack trace:", exc_info=True)
        raise
    finally:
        # Ensure Spark session is stopped
        try:
            spark = SparkSession.getActiveSession()
            if spark:
                spark.stop()
                logger.info("Spark session stopped")
        except:
            pass
