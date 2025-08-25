#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, LongType
)

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, read_file,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings
)
from utils.config import SPARK_CONFIGS, INPUT_PATHS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY

logger = setup_logging(__name__)

# Schema definition for salary_history
SALARY_HISTORY_SCHEMA = StructType([
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

@log_execution
def add_partitioning_columns(df):
    """Add partitioning columns for optimized storage (job-specific)"""
    logger.info("Adding partitioning columns for optimized storage")
    df = df.withColumn("year_effective_date", year(col("effective_date")))
    df = df.withColumn("month_effective_date", month(col("effective_date")))
    df = df.withColumn("day_effective_date", dayofmonth(col("effective_date")))
    return df

@log_execution
def salary_history_etl():
    """Main ETL process using shared utilities - MUCH SIMPLER NOW!"""
    logger.info("Starting Salary History Silver Layer ETL Process")
    logger.info("Using shared utilities for all operations")

    spark = create_spark_session(SPARK_CONFIGS["salary_history"])
    input_path = INPUT_PATHS["salary_history"]
    logger.info(f"Reading salary history data from: {input_path}")
    
    # Read Parquet with schema
    df = spark.read.schema(SALARY_HISTORY_SCHEMA).parquet(input_path)
    df = df.cache()
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")

    # Apply transformations using shared utilities
    df = transform_empty_strings(df)  # Shared utility
    df = add_timestamp_column(df)     # Shared utility
    df = apply_referential_integrity(df, REFERENTIAL_INTEGRITY["salary_history"])  # Shared utility
    df = clean_dataframe(df, CLEANING_RULES["salary_history"])  # Shared utility
    df = add_partitioning_columns(df)  # Job-specific

    output_path = OUTPUT_PATHS["salary_history"]
    logger.info(f"Writing cleaned data to silver layer: {output_path}")
    df.write.partitionBy("year_effective_date", "month_effective_date", "day_effective_date").parquet(
        output_path, mode="overwrite"
    )
    final_count = df.count()
    logger.info(f"Successfully processed {final_count} records")
    logger.info("Salary History ETL process completed successfully")
    return df

if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("SALARY HISTORY SILVER LAYER ETL JOB")
        logger.info("=" * 80)
        
        # Execute the ETL process
        result_df = salary_history_etl()
        
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
