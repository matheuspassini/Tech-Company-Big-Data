#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, avg, count, sum, round, countDistinct, collect_list, collect_set, size, split, min as spark_min, max as spark_max, when, expr, array_sort, array_contains, cast
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, read_file,
    clean_dataframe, apply_referential_integrity, add_timestamp_column,
    transform_empty_strings
)
from utils.config import SPARK_CONFIGS, INPUT_PATHS, OUTPUT_PATHS, CLEANING_RULES, REFERENTIAL_INTEGRITY

logger = setup_logging(__name__)

def load_silver_data(spark):
    """Load data from Silver layer"""
    logger.info("Loading data from Silver layer")
    
    # Use utility function for reading data with config paths
    df_employees = read_file(spark, INPUT_PATHS["department_analytics_employees"])
    df_departments = read_file(spark, INPUT_PATHS["department_analytics_departments"])
    
    logger.info(f"Loaded {df_employees.count()} employee records")
    logger.info(f"Loaded {df_departments.count()} department records")
    
    return df_employees, df_departments

@log_execution
def prepare_department_data(df_departments):
    """Rename department columns and prepare for join"""
    logger.info("Preparing department data for join")
    
    # Rename all department columns to have 'departments_' prefix
    for col_name in df_departments.columns:
        df_departments = df_departments.withColumnRenamed(col_name, f"departments_{col_name}")
    
    return df_departments

@log_execution
def join_employee_department_data(df_employees, df_departments):
    """Join employees and departments data"""
    logger.info("Joining employees and departments data")
    
    # Join employees and departments
    df_joined = df_employees.join(
        df_departments,
        df_employees.department_id == df_departments.departments_id,
        "inner"
    )
    
    logger.info(f"Joined data contains {df_joined.count()} records")
    
    return df_joined

@log_execution
def remove_duplicate_columns(df_joined):
    """Remove duplicate department_id column after join"""
    logger.info("Removing duplicate department_id column")
    
    # Remove the duplicate department_id column (departments_id) since we already have department_id from employees
    # Keep only the first department_id column (from employees) and remove the second one (departments_id)
    df_joined = df_joined.drop("departments_id")
    
    logger.info("Successfully removed duplicate departments_id column")
    
    return df_joined

@log_execution
def cast_data_types(df_joined):
    """Cast all columns to correct data types"""
    logger.info("Casting columns to correct data types")
    
    # Cast all columns used in aggregations to correct data types
    df_joined = df_joined.withColumn("departments_cost_center", col("departments_cost_center").cast(LongType()))
    df_joined = df_joined.withColumn("departments_budget", col("departments_budget").cast(LongType()))
    df_joined = df_joined.withColumn("salary", col("salary").cast(DoubleType()))
    df_joined = df_joined.withColumn("performance_score", col("performance_score").cast(DoubleType()))
    df_joined = df_joined.withColumn("projects_assigned", col("projects_assigned").cast(IntegerType()))
    df_joined = df_joined.withColumn("departments_region", col("departments_region").cast(StringType()))
    df_joined = df_joined.withColumn("department_id", col("department_id").cast(StringType()))
    df_joined = df_joined.withColumn("departments_name", col("departments_name").cast(StringType()))
    df_joined = df_joined.withColumn("position", col("position").cast(StringType()))
    
    return df_joined

@log_execution
def transform_education_levels(df_joined):
    """Transform education levels to numeric hierarchy"""
    logger.info("Transforming education levels to numeric hierarchy")
    
    # Label education level hierarchy for proper minimum calculation
    df_joined = df_joined.withColumn(
        "education",
        when(col("education") == "High School", 1)
        .when(col("education") == "Associate's Degree", 2)
        .when(col("education") == "Bachelor's Degree", 3)
        .when(col("education") == "Master's Degree", 4)
        .when(col("education") == "PhD", 5)
        .otherwise(0)
    )
    
    return df_joined

@log_execution
def create_department_analytics(df_joined):
    """Create department analytics with aggregations and calculated metrics"""
    logger.info("Creating department analytics with aggregations")
    
    # Create analytics table with aggregations
    df_analytics = df_joined.groupBy(
        col("departments_region").alias("region"),
        col("department_id").alias("department_id"),
        col("departments_name").alias("department_name")
    ).agg(
        # Budget and financial metrics
        sum("departments_budget").alias("total_budget"),
        sum("departments_cost_center").alias("total_cost_center"),
        
        # Employee metrics
        count("*").alias("total_employees"),
        countDistinct("position").alias("total_positions"),
        
        # Salary metrics
        sum("salary").alias("total_salary_cost"),
        avg("salary").alias("avg_salary"),
        
        # Performance metrics
        avg("performance_score").alias("avg_performance"),
        sum("projects_assigned").alias("total_projects")
    ).withColumn(
        "budget_available", col("total_budget") - col("total_cost_center")
    ).withColumn(
        "required_review_department_cost", col("budget_available") < 0
    ).withColumn(
        "salary_budget_ratio", round(col("total_salary_cost") / col("total_budget") * 100, 2)
    ).withColumn(
        "budget_per_employee", round(col("total_budget") / col("total_employees"), 2)
    ).withColumn(
        "cost_center_per_employee", round(col("total_cost_center") / col("total_employees"), 2)
    ).withColumn(
        "gold_processed_at", current_timestamp()
    ).orderBy(col("total_employees").desc())
    
    analytics_count = df_analytics.count()
    logger.info(f"Created analytics for {analytics_count} departments")
    
    return df_analytics

@log_execution
def write_gold_layer(df_analytics):
    """Write analytics data to Gold layer"""
    logger.info("Writing analytics data to Gold layer")
    
    # Write to Gold layer in Parquet format with partitioning
    output_path_parquet = "hdfs://master:8080/opt/spark/data/gold_layer/department_analytics.parquet"
    logger.info(f"Writing Parquet to: {output_path_parquet}")
    
    df_analytics.write.partitionBy("region", "required_review_department_cost", "department_name").parquet(
        output_path_parquet, mode="overwrite"
    )
    
    logger.info("Successfully wrote analytics data to Gold layer")

@log_execution
def department_analytics_etl():
    """Main ETL process for department analytics"""
    logger.info("Starting Department Analytics Gold Layer ETL Process")
    
    spark = create_spark_session(SPARK_CONFIGS.get("department_analytics", {}))
    
    # Load data from Silver layer
    df_employees, df_departments = load_silver_data(spark)
    
    # Prepare department data
    df_departments = prepare_department_data(df_departments)
    
    # Join data
    df_joined = join_employee_department_data(df_employees, df_departments)
    
    # Remove duplicate columns
    df_joined = remove_duplicate_columns(df_joined)
    
    # Cast data types
    df_joined = cast_data_types(df_joined)
    
    # Transform education levels
    df_joined = transform_education_levels(df_joined)
    
    # Create analytics
    df_analytics = create_department_analytics(df_joined)
    
    # Write to Gold layer
    write_gold_layer(df_analytics)
    
    logger.info("Department Analytics ETL process completed successfully")
    return df_analytics

if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("DEPARTMENT ANALYTICS GOLD LAYER ETL JOB")
        logger.info("=" * 80)
        
        # Execute the ETL process
        result_df = department_analytics_etl()
        
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