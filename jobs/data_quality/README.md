# Data Quality Report

## Overview

This job performs data quality assessment on all files in the bronze layer, analyzing missing values and generating quality reports. Uses shared utilities and centralized configurations.

## Features

- **Multi-format Support**: Handles CSV, JSON, and Parquet files
- **Quality Assessment**: Analyzes missing values across all columns
- **Quality Flags**: 
  - **Green**: 0-10% missing values (high quality)
  - **Yellow**: 10-30% missing values (medium quality)
  - **Red**: >30% missing values (low quality)
- **Partitioned Reports**: Quality reports partitioned by flag for easy analysis
- **Comprehensive Coverage**: Analyzes all 6 data sources (departments, clients, employees, tasks, salary_history, projects)
- **Shared Utilities**: Uses centralized utilities for logging and data processing

## Execution

### Individual Job (Cluster Mode)
```bash
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/data_quality/data_quality_report.py
```

### Full Pipeline
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline_1.py
```

## Output

The job generates partitioned quality reports in:
```
hdfs://master:8080/opt/spark/data/bronze_layer/data_quality/data_quality_report/
├── flag=Green/
├── flag=Yellow/
└── flag=Red/
```

## Algorithm

1. **File Processing**: Reads each file from bronze layer
2. **Null Detection**: Identifies missing values using multiple conditions
3. **Percentage Calculation**: Computes missing value percentage per column
4. **Flag Assignment**: Assigns quality flags based on thresholds
5. **Report Generation**: Creates partitioned output by quality flag

## Data Sources

- departments.csv
- clients.csv
- employees.json
- tasks.json
- salary_history.parquet
- projects.parquet

## Shared Utilities

This job uses the following shared utilities:
- **Logging**: Centralized logging with execution tracking
- **Data Processing**: Common data cleaning and transformation functions
- **Configuration**: Centralized Spark configurations and paths

## Monitoring

### Monitor Job Execution in Real-time
```bash
# Monitor YARN applications with watch (real-time updates every 3 seconds)
watch -n 3 'docker exec tech-data-lake-master yarn application -list'

# Monitor specific application logs
docker exec tech-data-lake-master yarn logs -applicationId <application_id>
```

### Web Interfaces
- **YARN Web UI**: http://localhost:8081
- **Spark History Server**: http://localhost:18081
