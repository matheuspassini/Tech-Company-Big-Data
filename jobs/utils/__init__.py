#!/usr/bin/env python3

"""
Utils module for Spark jobs
Contains shared functions and configurations
"""

from .spark_utils import (
    setup_logging,
    log_execution,
    create_spark_session,
    read_file,
    check_null_percentage,
    clean_dataframe,
    apply_referential_integrity,
    add_timestamp_column,
    transform_empty_strings,
    drop_unnecessary_columns
)

from .config import (
    DATA_QUALITY_FILES,
    SPARK_CONFIGS,
    INPUT_PATHS,
    OUTPUT_PATHS,
    QUALITY_THRESHOLDS,
    CLEANING_RULES,
    REFERENTIAL_INTEGRITY,
    COLUMNS_TO_DROP
)

__all__ = [
    'setup_logging',
    'log_execution', 
    'create_spark_session',
    'read_file',
    'check_null_percentage',
    'clean_dataframe',
    'apply_referential_integrity',
    'add_timestamp_column',
    'transform_empty_strings',
    'drop_unnecessary_columns',
    'DATA_QUALITY_FILES',
    'SPARK_CONFIGS',
    'INPUT_PATHS',
    'OUTPUT_PATHS',
    'QUALITY_THRESHOLDS',
    'CLEANING_RULES',
    'REFERENTIAL_INTEGRITY',
    'COLUMNS_TO_DROP'
]
