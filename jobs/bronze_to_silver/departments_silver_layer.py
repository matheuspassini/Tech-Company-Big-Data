#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, read_file,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings
)
from utils.config import SPARK_CONFIGS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY

logger = setup_logging(__name__)

@log_execution
def add_partitioning_columns(df):
    """Add partitioning columns for optimized storage"""
    logger.info("Adding partitioning columns for optimized storage")
    
    df = df.withColumn("year_founded_date", year(col("founded_date")))
    df = df.withColumn("month_founded_date", month(col("founded_date")))
    df = df.withColumn("day_founded_date", dayofmonth(col("founded_date")))
    
    return df

@log_execution
def departments_etl():
    """Main ETL process using shared utilities - MUCH SIMPLER NOW!"""
    logger.info("Starting Departments Silver Layer ETL Process")
    logger.info("Using shared utilities for all operations")
    
    # Create Spark session using shared utility
    spark = create_spark_session(SPARK_CONFIGS["departments"])
    
    # Read data using shared utility
    input_path = "hdfs://master:8080/opt/spark/data/bronze_layer/departments.csv"
    logger.info(f"Reading departments data from: {input_path}")
    df = read_file(spark, input_path)
    df = df.cache()
    
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")
    
    # Apply transformations using shared utilities
    df = transform_empty_strings(df)  # Shared utility
    df = add_timestamp_column(df)     # Shared utility
    df = apply_referential_integrity(df, REFERENTIAL_INTEGRITY["departments"])  # Shared utility
    df = clean_dataframe(df, CLEANING_RULES["departments"])  # Shared utility
    df = add_partitioning_columns(df)  # Job-specific
    
    # Write to silver layer using shared config
    output_path = OUTPUT_PATHS["departments"]
    logger.info(f"Writing cleaned data to silver layer: {output_path}")
    
    df.write.partitionBy("year_founded_date", "month_founded_date", "day_founded_date").parquet(
        output_path, mode="overwrite"
    )
    
    final_count = df.count()
    logger.info(f"Successfully processed {final_count} records")
    logger.info("Departments ETL process completed successfully")
    
    return df

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("DEPARTMENTS SILVER LAYER ETL JOB STARTING")
    logger.info("=" * 80)

    try:
        df_result = departments_etl()
        
        logger.info("=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total records processed: {df_result.count()}")
        logger.info(f"Output saved to: {OUTPUT_PATHS['departments']}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        spark = SparkSession.getActiveSession()
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")

    logger.info("=" * 80)
    logger.info("DEPARTMENTS SILVER LAYER ETL JOB COMPLETED")
    logger.info("=" * 80)
