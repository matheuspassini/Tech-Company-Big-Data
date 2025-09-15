#!/usr/bin/env python3

import logging
import time
import functools
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim, lower, size, array, expr, current_timestamp
from pyspark.sql.types import ArrayType

def setup_logging(name: str = __name__) -> logging.Logger:
    """Setup logging configuration for Spark jobs"""
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler()],
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )
    return logging.getLogger(name)

def log_execution(function):
    """Decorator to log function execution with timing and record count"""
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        logger = setup_logging()
        start_time = time.time()
        logger.info(f"Starting execution of {function.__name__}")
        
        try:
            result = function(*args, **kwargs)
            
            if hasattr(result, 'count'):
                record_count = result.count()
                logger.info(f"Function {function.__name__} processed {record_count} records")
            else:
                logger.info(f"Function {function.__name__} completed successfully")
            
            execution_time = time.time() - start_time
            logger.info(f"Function {function.__name__} completed in {execution_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Function {function.__name__} failed after {execution_time:.2f} seconds")
            logger.error(f"   Error: {str(e)}")
            raise
    
    return wrapper

def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with standard configuration"""
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_file(spark: SparkSession, file_path: str) -> DataFrame:
    """Read file based on extension with standard options"""
    logger = setup_logging()
    
    if file_path.endswith(".csv"):
        logger.debug(f"   Reading CSV file with multiline and escape options")
        return spark.read.option("multiline", "true").option("escape", "\"").csv(file_path, header=True)
    elif file_path.endswith(".json"):
        logger.debug(f"   Reading JSON file with multiline option")
        return spark.read.option("multiline", "true").json(file_path)
    elif file_path.endswith(".parquet"):
        logger.debug(f"   Reading Parquet file")
        return spark.read.parquet(file_path)
    else:
        logger.error(f"   Unsupported file format: {file_path}")
        raise ValueError(f"Unsupported file format: {file_path}")

def check_null_percentage(df: DataFrame, column: str, red_threshold: float, yellow_threshold: float) -> Dict[str, Any]:
    """Check null percentage for a specific column
    
    Args:
        df: Spark DataFrame to analyze
        column: Column name to check
        red_threshold: Percentage threshold for Red flag (e.g., 30.0)
        yellow_threshold: Percentage threshold for Yellow flag (e.g., 10.0)
    
    Returns:
        Dict with null_count, total_count, null_percentage, and flag
    """
    total_rows = df.count()
    column_type = df.schema[column].dataType
    
    # Log column type for debugging
    logger = setup_logging()
    logger.debug(f"      Analyzing column '{column}' (Type: {column_type})")
    
    if str(column_type).startswith('ArrayType'):
        logger.debug(f"         ArrayType detected - checking null and empty arrays")
        missing_count = df.filter(
            (col(column).isNull()) | (size(col(column)) == 0)
        ).count()
    else:
        logger.debug(f"         Standard type - checking all null conditions")
        missing_count = df.filter(
            (col(column).isNull()) |
            (col(column) == "") |
            (trim(col(column)) == "") |
            (lower(col(column)) == "nan") |
            (col(column) == "null") |
            (col(column) == "NULL") |
            (col(column) == "None") |
            (col(column) == "N/A") |
            (col(column) == "n/a") |
            (col(column) == "[]") |
            (col(column) == "{}") |
            (col(column) == "undefined") |
            (col(column) == "UNDEFINED")
        ).count()
    
    null_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
    
    if null_percentage > red_threshold:
        flag = "Red"
    elif null_percentage > yellow_threshold:
        flag = "Yellow"
    else:
        flag = "Green"
    
    logger.debug(f"         Analysis complete: {missing_count}/{total_rows} nulls ({null_percentage:.2f}%) -> {flag}")
    
    return {
        "null_count": missing_count,
        "total_count": total_rows,
        "null_percentage": round(null_percentage, 2),
        "flag": flag
    }

@log_execution
def clean_dataframe(df: DataFrame, cleaning_rules: Dict[str, Any]) -> DataFrame:
    """Generic data cleaning function that can be reused across jobs
    
    Args:
        df: DataFrame to clean
        cleaning_rules: Dictionary with cleaning rules
            {
                "null_dates": ["col1", "col2"],
                "null_floats": ["col3", "col4"], 
                "null_integers": ["col5", "col6"],
                "null_strings": ["col7", "col8"],
                "arrays": ["col9"]
            }
    
    Returns:
        Cleaned DataFrame
    """
    logger = setup_logging()
    logger.info("Applying data cleaning rules with fillna()")
    
    # Build fillna dictionary for batch processing
    fillna_dict = {}
    
    # Clean null dates
    if "null_dates" in cleaning_rules:
        for col_name in cleaning_rules["null_dates"]:
            fillna_dict[col_name] = "0000-01-01"
    
    # Clean null floats
    if "null_floats" in cleaning_rules:
        for col_name in cleaning_rules["null_floats"]:
            fillna_dict[col_name] = 0.0
    
    # Clean null integers
    if "null_integers" in cleaning_rules:
        for col_name in cleaning_rules["null_integers"]:
            fillna_dict[col_name] = 0
    
    # Clean null strings
    if "null_strings" in cleaning_rules:
        for col_name in cleaning_rules["null_strings"]:
            fillna_dict[col_name] = "Unknown"
    
    # Clean null booleans
    if "null_booleans" in cleaning_rules:
        for col_name in cleaning_rules["null_booleans"]:
            fillna_dict[col_name] = False
    
    # Apply all null replacements in a single operation
    if fillna_dict:
        logger.info(f"Applying fillna() to {len(fillna_dict)} columns in single operation")
        df = df.fillna(fillna_dict)
    
    # Clean arrays
    if "arrays" in cleaning_rules:
        for col_name in cleaning_rules["arrays"]:
            if col_name in df.columns:
                if isinstance(df.schema[col_name].dataType, ArrayType):
                    # For real arrays: replace null arrays with empty arrays and filter out null/empty elements
                    df = df.withColumn(col_name,
                        when(col(col_name).isNull(), array()).otherwise(
                            expr(f"filter({col_name}, x -> x is not null and trim(x) != '')")
                        )
                    )
                    logger.info(f"Cleaned array column: {col_name} (integrated)")
                else:
                    # For arrays stored as strings: replace null with "Not available"
                    df = df.withColumn(col_name, 
                        when(col(col_name).isNull(), "Not available").otherwise(col(col_name))
                    )
    
    return df

@log_execution
def apply_referential_integrity(df: DataFrame, required_columns: list) -> DataFrame:
    """Apply referential integrity constraints using dropna()
    
    Args:
        df: DataFrame to apply constraints
        required_columns: List of columns that cannot be null
    
    Returns:
        DataFrame with constraints applied
    """
    logger = setup_logging()
    logger.info(f"Applying referential integrity constraints for columns: {required_columns}")
    
    initial_count = df.count()
    
    # Use dropna() - single operation
    df = df.dropna(subset=required_columns)
    
    final_count = df.count()
    dropped_count = initial_count - final_count
    
    logger.info(f"Records after referential integrity: {final_count} (dropped {dropped_count} records)")
    return df

@log_execution
def add_timestamp_column(df: DataFrame, column_name: str = "received_at") -> DataFrame:
    """Add timestamp column to DataFrame
    
    Args:
        df: DataFrame to add timestamp
        column_name: Name of the timestamp column
    
    Returns:
        DataFrame with timestamp column
    """
    logger = setup_logging()
    logger.info(f"Adding timestamp column: {column_name}")
    
    return df.withColumn(column_name, current_timestamp())

@log_execution
def transform_empty_strings(df: DataFrame) -> DataFrame:
    """Transform empty strings to null values
    
    Args:
        df: DataFrame to transform
    
    Returns:
        DataFrame with empty strings transformed to nulls
    """
    logger = setup_logging()
    logger.info("Transforming empty strings to null values")
    
    # Get column types to skip array columns
    
    return df.select([
        when(
            (col(c) == "") |
            (col(c) == "[]") |
            (col(c) == "{}") |
            (col(c) == "null") |
            (col(c) == "NULL") |
            (col(c) == "None") |
            (col(c) == "N/A") |
            (col(c) == " ") |
            (col(c) == "n/a") |
            (col(c) == "undefined") |
            (col(c) == "UNDEFINED"),
            None
        ).otherwise(col(c)).alias(c) 
        if not isinstance(df.schema[c].dataType, ArrayType)
        else col(c)  # Skip transformation for array columns
        for c in df.columns
    ])


@log_execution
def drop_unnecessary_columns(df: DataFrame, columns_to_drop: list) -> DataFrame:
    """Drop unnecessary columns from DataFrame
    
    Args:
        df: DataFrame to drop columns from
        columns_to_drop: List of column names to drop
    
    Returns:
        DataFrame with specified columns dropped
    """
    logger = setup_logging()
    logger.info(f"Dropping unnecessary columns: {columns_to_drop}")
    
    return df.drop(*columns_to_drop)
