#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col, when, to_date, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, ArrayType, LongType, DateType
)

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings
)
from utils.config import SPARK_CONFIGS, INPUT_PATHS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY

logger = setup_logging(__name__)

# Schema definition for projects
PROJECTS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("budget", LongType(), True),
    StructField("priority", StringType(), True),
    StructField("description", StringType(), True),
    StructField("technologies", ArrayType(StringType()), True),
    StructField("team_size", LongType(), True),
    StructField("risk_level", StringType(), True),
    StructField("success_metrics", StringType(), True),
    StructField("actual_cost", DoubleType(), True),
    StructField("estimated_completion_date", DateType(), True),
    StructField("project_type", StringType(), True),
    StructField("complexity", StringType(), True),
    StructField("dependencies", LongType(), True),
    StructField("milestones_completed", LongType(), True),
    StructField("total_milestones", LongType(), True),
    StructField("quality_score", DoubleType(), True),
    StructField("customer_feedback", StringType(), True),
    StructField("resource_utilization", DoubleType(), True)
])


@log_execution
def convert_numeric_columns(df):
    """Convert integer columns to appropriate numeric types"""
    logger.info("Converting numeric columns to appropriate types")
    # Only convert if columns exist and are not null
    if "budget" in df.columns:
        df = df.withColumn("budget", col("budget").cast("double"))
    if "team_size" in df.columns:
        df = df.withColumn("team_size", col("team_size").cast("integer"))
    if "dependencies" in df.columns:
        df = df.withColumn("dependencies", col("dependencies").cast("integer"))
    if "milestones_completed" in df.columns:
        df = df.withColumn("milestones_completed", col("milestones_completed").cast("integer"))
    if "total_milestones" in df.columns:
        df = df.withColumn("total_milestones", col("total_milestones").cast("integer"))
    return df


@log_execution
def add_partitioning_columns(df):
    """Add partitioning columns for optimized storage (job-specific)"""
    logger.info("Adding partitioning columns for optimized storage")
    # Extract partitioning columns directly from DateType
    df = df.withColumn("year_start_date", year(col("start_date")))
    df = df.withColumn("month_start_date", month(col("start_date")))
    df = df.withColumn("day_start_date", dayofmonth(col("start_date")))
    return df

@log_execution
def projects_etl():
    """Main ETL process"""
    logger.info("Starting Projects Silver Layer ETL Process")

    spark = create_spark_session(SPARK_CONFIGS["projects"])
    input_path = INPUT_PATHS["projects"]
    logger.info(f"Reading projects data from: {input_path}")
    
    # Read Parquet with schema
    df = spark.read.schema(PROJECTS_SCHEMA).parquet(input_path)
    df = df.cache()
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count}")

    # Apply transformations using shared utilities
    logger.info("Applying transformations...")
    df = convert_numeric_columns(df)  # Convert integer columns to appropriate numeric types
    df = transform_empty_strings(df)  # Shared utility
    df = add_timestamp_column(df)     # Shared utility
    df = apply_referential_integrity(df, REFERENTIAL_INTEGRITY["projects"])  # Shared utility
    df = clean_dataframe(df, CLEANING_RULES["projects"])  # Shared utility (includes array cleaning)
    df = add_partitioning_columns(df)  # Job-specific
    logger.info("Transformations completed")

    output_path = OUTPUT_PATHS["projects"]
    logger.info(f"Writing cleaned data to silver layer: {output_path}")
    
    # Write data to silver layer with partitioning (status + date)
    df.write.partitionBy("status", "year_start_date", "month_start_date", "day_start_date").parquet(
        output_path, mode="overwrite"
    )
    
    # Count records before returning (cache to avoid recomputation)
    df.cache()
    final_count = df.count()
    logger.info(f"Successfully processed {final_count} records")
    logger.info("Projects ETL process completed successfully")
    return df

if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("PROJECTS SILVER LAYER ETL JOB")
        logger.info("=" * 80)
        
        # Execute the ETL process
        result_df = projects_etl()
        
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
