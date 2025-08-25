#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, read_file,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings
)
from utils.config import SPARK_CONFIGS, INPUT_PATHS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY

logger = setup_logging(__name__)

# Schema definition for tasks
TASKS_SCHEMA = StructType([
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

@log_execution
def add_partitioning_columns(df):
    """Add partitioning columns for optimized storage (job-specific)"""
    logger.info("Adding partitioning columns for optimized storage")
    df = df.withColumn("year_created_date", year(col("created_date")))
    df = df.withColumn("month_created_date", month(col("created_date")))
    df = df.withColumn("day_created_date", dayofmonth(col("created_date")))
    return df

@log_execution
def tasks_etl():
    """Main ETL process using shared utilities - MUCH SIMPLER NOW!"""
    logger.info("Starting Tasks Silver Layer ETL Process")
    logger.info("Using shared utilities for all operations")

    spark = create_spark_session(SPARK_CONFIGS["tasks"])
    input_path = INPUT_PATHS["tasks"]
    logger.info(f"Reading tasks data from: {input_path}")
    
    # Read JSON with schema and multiLine
    df = spark.read.schema(TASKS_SCHEMA).json(input_path, multiLine=True)
    df = df.cache()
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")

    # Apply transformations using shared utilities
    df = transform_empty_strings(df)  # Shared utility
    df = add_timestamp_column(df)     # Shared utility
    df = apply_referential_integrity(df, REFERENTIAL_INTEGRITY["tasks"])  # Shared utility
    df = clean_dataframe(df, CLEANING_RULES["tasks"])  # Shared utility
    df = add_partitioning_columns(df)  # Job-specific

    output_path = OUTPUT_PATHS["tasks"]
    logger.info(f"Writing cleaned data to silver layer: {output_path}")
    df.write.partitionBy("year_created_date", "month_created_date", "day_created_date").parquet(
        output_path, mode="overwrite"
    )
    final_count = df.count()
    logger.info(f"Successfully processed {final_count} records")
    logger.info("Tasks ETL process completed successfully")
    return df

if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("TASKS SILVER LAYER ETL JOB")
        logger.info("=" * 80)
        
        # Execute the ETL process
        result_df = tasks_etl()
        
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
