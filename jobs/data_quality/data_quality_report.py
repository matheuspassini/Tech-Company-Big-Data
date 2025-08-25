#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from utils.spark_utils import (
    setup_logging, log_execution, create_spark_session, 
    read_file, check_null_percentage
)
from utils.config import (
    DATA_QUALITY_FILES, SPARK_CONFIGS, OUTPUT_PATHS, QUALITY_THRESHOLDS
)

logger = setup_logging(__name__)

@log_execution
def null_data_report():
    """Data quality report using shared utilities"""
    logger.info("Starting Data Quality Analysis Process")
    logger.info(f"Analyzing {len(DATA_QUALITY_FILES)} files for data quality")
    
    spark = create_spark_session(SPARK_CONFIGS["data_quality"])
    logger.info(f"Spark session created: {SPARK_CONFIGS['data_quality']}")
    
    quality_data_report = {
        "datasource": [],
        "column": [],
        "null_count": [],
        "count": [],
        "null_percentage": [],
        "flag": []
    }
    
    total_files = len(DATA_QUALITY_FILES)
    for file_index, file_path in enumerate(DATA_QUALITY_FILES, 1):
        file_name = file_path.split('/')[-1]
        logger.info(f"Processing file {file_index}/{total_files}: {file_name}")
        
        # Read file using shared utility
        logger.info(f"   Reading file: {file_path}")
        df = read_file(spark, file_path)
        df = df.cache()
        
        total_rows = df.count()
        total_columns = len(df.columns)
        logger.info(f"   File loaded: {total_rows} rows, {total_columns} columns")
        
        # Check each column using shared utility
        logger.info(f"   Analyzing {total_columns} columns for data quality")
        for col_index, column in enumerate(df.columns, 1):
            logger.info(f"      Column {col_index}/{total_columns}: {column}")
            result = check_null_percentage(
                df, 
                column, 
                red_threshold=QUALITY_THRESHOLDS["red"],
                yellow_threshold=QUALITY_THRESHOLDS["yellow"]
            )
            
            # Log column results
            logger.info(f"         Results: {result['null_count']}/{result['total_count']} nulls ({result['null_percentage']}%) - Flag: {result['flag']}")
            
            quality_data_report["datasource"].append(file_name)
            quality_data_report["column"].append(column)
            quality_data_report["null_count"].append(result["null_count"])
            quality_data_report["count"].append(result["total_count"])
            quality_data_report["null_percentage"].append(result["null_percentage"])
            quality_data_report["flag"].append(result["flag"])
        
        logger.info(f"   File {file_name} analysis completed")
    
    if not quality_data_report["datasource"]:
        logger.warning("No data sources found for analysis")
        return None
    
    # Create DataFrame schema
    logger.info("Creating quality report DataFrame")
    quality_schema = StructType([
        StructField("datasource", StringType(), True),
        StructField("column", StringType(), True),
        StructField("null_count", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("null_percentage", DoubleType(), True),
        StructField("flag", StringType(), True)
    ])
    
    # Create DataFrame
    df_quality_report = spark.createDataFrame(
        list(zip(
            quality_data_report["datasource"],
            quality_data_report["column"],
            quality_data_report["null_count"],
            quality_data_report["count"],
            quality_data_report["null_percentage"],
            quality_data_report["flag"]
        )),
        quality_schema
    )
    
    total_quality_records = len(quality_data_report["datasource"])
    logger.info(f"Quality report DataFrame created with {total_quality_records} records")
    
    # Analyze flags distribution
    flag_counts = {}
    for flag in quality_data_report["flag"]:
        flag_counts[flag] = flag_counts.get(flag, 0) + 1
    
    logger.info("Data Quality Summary:")
    for flag, count in flag_counts.items():
        logger.info(f"   {flag}: {count} columns")
    
    # Write report
    output_path = OUTPUT_PATHS["data_quality"]
    logger.info(f"Writing quality report to: {output_path}")
    df_quality_report.write.partitionBy("flag").parquet(output_path, mode="overwrite")
    
    logger.info("Data Quality Analysis Process Completed Successfully!")
    logger.info(f"Report saved with partitioning by flag")
    
    return df_quality_report

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("DATA QUALITY ANALYSIS JOB STARTING")
    logger.info("=" * 80)
    
    try:
        df_quality_report = null_data_report()
        
        if df_quality_report:
            logger.info("=" * 80)
            logger.info("FINAL SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total quality records generated: {df_quality_report.count()}")
            logger.info(f"Files analyzed: {len(DATA_QUALITY_FILES)}")
            logger.info(f"Report saved to: {OUTPUT_PATHS['data_quality']}")
            logger.info("=" * 80)
        else:
            logger.error("No quality report generated")
            
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
    logger.info("DATA QUALITY ANALYSIS JOB COMPLETED")
    logger.info("=" * 80)
