# Shared Utilities

This directory contains centralized utilities used by all Spark jobs in the data pipeline. These utilities provide consistent functionality across all jobs and are automatically distributed to containers via `utils.zip`.

## Structure

```
utils/
├── __init__.py          # Package initialization and exports
├── config.py            # Centralized configurations and paths
├── spark_utils.py       # Common Spark utilities and functions
└── README.md           # This file
```

## Files Overview

### `__init__.py`
Package initialization file that exports key utilities for easy importing:
- **Exports**: Main functions and configurations for direct import
- **Version**: Package version information
- **Dependencies**: Internal module dependencies

### `config.py`
Centralized configuration management for all jobs:
- **SPARK_CONFIGS**: Spark application configurations for each job
- **OUTPUT_PATHS**: HDFS output paths for all data layers
- **CLEANING_RULES**: Data cleaning rules for each entity
- **REFERENTIAL_INTEGRITY**: Referential integrity rules for data validation

### `spark_utils.py`
Core utility functions used across all jobs:
- **Logging**: Centralized logging setup and decorators
- **Spark Session**: Spark session creation and management
- **Data Processing**: Common data transformation functions
- **File Operations**: Reading and writing data files
- **Data Quality**: Data cleaning and validation functions

## Key Features

### Centralized Logging
```python
from utils.spark_utils import setup_logging, log_execution

logger = setup_logging(__name__)

@log_execution
def my_function(df):
    # Function automatically logged with timing and record counts
    return df
```

### Spark Session Management
```python
from utils.spark_utils import create_spark_session
from utils.config import SPARK_CONFIGS

spark = create_spark_session(SPARK_CONFIGS["employees"])
```

### Data Processing Functions
```python
from utils.spark_utils import (
    read_file, clean_dataframe, apply_referential_integrity,
    add_timestamp_column, transform_empty_strings
)

# Read data
df = read_file(spark, input_path)

# Apply transformations
df = transform_empty_strings(df)
df = add_timestamp_column(df)
df = apply_referential_integrity(df, rules)
df = clean_dataframe(df, cleaning_rules)
```

### Configuration Management
```python
from utils.config import SPARK_CONFIGS, OUTPUT_PATHS, CLEANING_RULES

# Use centralized configurations
app_config = SPARK_CONFIGS["employees"]
output_path = OUTPUT_PATHS["employees"]
cleaning_rules = CLEANING_RULES["employees"]
```

## Usage in Jobs

All jobs automatically use these utilities through the `utils.zip` file:

```python
# Jobs automatically import from utils.zip
from utils.spark_utils import setup_logging, create_spark_session
from utils.config import SPARK_CONFIGS, OUTPUT_PATHS

logger = setup_logging(__name__)
spark = create_spark_session(SPARK_CONFIGS["job_name"])
```

## Distribution

The utilities are automatically distributed to all containers:

1. **Pipeline Creation**: `run_pipeline_1.py` creates `utils.zip`
2. **HDFS Upload**: `utils.zip` uploaded to HDFS for distribution
3. **Job Execution**: All jobs use `--py-files utils.zip` for access
4. **Container Access**: Utilities available in all YARN containers

## Benefits

- **Consistency**: All jobs use the same utilities and configurations
- **Maintainability**: Centralized code reduces duplication
- **Reliability**: Tested utilities used across all jobs
- **Performance**: Optimized functions for data processing
- **Observability**: Centralized logging and monitoring
- **Scalability**: Easy to add new utilities and configurations

## Adding New Utilities

To add new utilities:

1. **Add Function**: Add new function to `spark_utils.py`
2. **Add Config**: Add configuration to `config.py` if needed
3. **Export**: Add export to `__init__.py` if needed
4. **Test**: Test with individual jobs
5. **Deploy**: Utilities automatically available in next pipeline run

## Logging Features

All utilities include comprehensive logging:

- **Execution Timing**: Automatic measurement of function execution time
- **Record Counting**: Automatic counting of records processed
- **Structured Logs**: Consistent log format with timestamps
- **Error Handling**: Detailed error logging and tracking
- **Performance Monitoring**: Track execution time and data processing metrics

## Data Quality Functions

Centralized data quality functions:

- **Null Handling**: Consistent null value handling across all data types
- **Data Validation**: Referential integrity and data validation
- **Data Cleaning**: Standardized data cleaning operations
- **Quality Assessment**: Data quality monitoring and reporting

## Monitoring

### Monitor Job Execution in Real-time
```bash
# Monitor YARN applications with watch (real-time updates every 3 seconds)
watch -n 3 'docker exec tech-data-lake-master yarn application -list'

# Monitor applications with different intervals
watch -n 5 'docker exec tech-data-lake-master yarn application -list'  # 5 seconds
watch -n 1 'docker exec tech-data-lake-master yarn application -list'  # 1 second

# Monitor specific application logs
docker exec tech-data-lake-master yarn logs -applicationId <application_id>
```

### Web Interfaces
- **YARN Web UI**: http://localhost:8081
- **Spark History Server**: http://localhost:18081
