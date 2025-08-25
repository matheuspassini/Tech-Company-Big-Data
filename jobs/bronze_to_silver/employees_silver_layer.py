#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType
)

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, read_file,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings, drop_unnecessary_columns
)
from utils.config import SPARK_CONFIGS, INPUT_PATHS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY, COLUMNS_TO_DROP

logger = setup_logging(__name__)

# Schema definition for employees
EMPLOYEE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("position", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("performance_score", FloatType(), True),
    StructField("skills", StringType(), True),
    StructField("education", StringType(), True),
    StructField("years_experience", IntegerType(), True),
    StructField("certifications", StringType(), True),
    StructField("projects_assigned", IntegerType(), True),
    StructField("is_manager", BooleanType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("emergency_contact", StringType(), True),
    StructField("emergency_phone", StringType(), True),
    StructField("work_location", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("last_review_date", StringType(), True),
    StructField("next_review_date", StringType(), True),
    StructField("attendance_rate", FloatType(), True),
    StructField("overtime_hours", IntegerType(), True),
    StructField("training_hours", IntegerType(), True),
    StructField("sick_days", IntegerType(), True),
    StructField("vacation_days_used", IntegerType(), True),
    StructField("vacation_days_remaining", IntegerType(), True),
])



@log_execution
def add_partitioning_columns(df):
    """Add partitioning columns for optimized storage (job-specific)"""
    logger.info("Adding partitioning columns for optimized storage")
    df = df.withColumn("year_hire_date", year(col("hire_date")))
    df = df.withColumn("month_hire_date", month(col("hire_date")))
    df = df.withColumn("day_hire_date", dayofmonth(col("hire_date")))
    return df

@log_execution
def employees_etl():
    """Main ETL process using shared utilities - MUCH SIMPLER NOW!"""
    logger.info("Starting Employees Silver Layer ETL Process")
    logger.info("Using shared utilities for all operations")

    spark = create_spark_session(SPARK_CONFIGS["employees"])
    input_path = INPUT_PATHS["employees"]
    logger.info(f"Reading employees data from: {input_path}")
    
    # Read JSON with schema
    df = spark.read.schema(EMPLOYEE_SCHEMA).json(input_path, multiLine=True)
    df = df.cache()
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")

    # Apply transformations using shared utilities
    df = transform_empty_strings(df)  # Shared utility
    df = add_timestamp_column(df)     # Shared utility
    df = apply_referential_integrity(df, REFERENTIAL_INTEGRITY["employees"])  # Shared utility
    df = clean_dataframe(df, CLEANING_RULES["employees"])  # Shared utility
    df = drop_unnecessary_columns(df, COLUMNS_TO_DROP["employees"])  # Shared utility
    df = add_partitioning_columns(df)  # Job-specific

    output_path = OUTPUT_PATHS["employees"]
    logger.info(f"Writing cleaned data to silver layer: {output_path}")
    df.write.partitionBy("year_hire_date", "month_hire_date", "day_hire_date").parquet(
        output_path, mode="overwrite"
    )
    final_count = df.count()
    logger.info(f"Successfully processed {final_count} records")
    logger.info("Employees ETL process completed successfully")
    return df

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("EMPLOYEES SILVER LAYER ETL JOB STARTING")
    logger.info("=" * 80)
    
    try:
        # Execute the ETL process
        result_df = employees_etl()
        
        # Final summary
        logger.info("=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        final_count = result_df.count()
        logger.info(f"Total records processed: {final_count}")
        logger.info(f"Output saved to: {OUTPUT_PATHS['employees']}")
        logger.info("=" * 80)
        
        # Stop Spark session
        logger.info("Stopping Spark session")
        result_df.sparkSession.stop()
        logger.info("Spark session stopped")
        
        logger.info("=" * 80)
        logger.info("EMPLOYEES SILVER LAYER ETL JOB COMPLETED")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error during ETL process: {str(e)}")
        logger.error("=" * 80)
        logger.error("EMPLOYEES SILVER LAYER ETL JOB FAILED")
        logger.error("=" * 80)
        raise
