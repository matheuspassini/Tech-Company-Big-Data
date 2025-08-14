# Data Quality System

## Overview
The Data Quality system provides automated assessment and monitoring of data quality across all data sources in the Bronze Layer. It analyzes missing values, assigns quality flags, and generates partitioned reports for easy analysis.

## Features

### 🔍 **Multi-Format Support**
- **CSV Files**: Handles comma-separated values with proper escaping
- **JSON Files**: Processes JSON arrays with multiline support
- **Parquet Files**: Reads optimized columnar format

### 🏷️ **Quality Flags**
- **🟢 Green**: 0-10% missing values (High Quality)
- **🟡 Yellow**: 10-30% missing values (Medium Quality)  
- **🔴 Red**: >30% missing values (Low Quality)

### 📊 **Comprehensive Analysis**
- **All Data Sources**: Analyzes 6 data sources (departments, clients, employees, tasks, salary_history, projects)
- **All Columns**: Evaluates every column in each dataset
- **Missing Value Detection**: Identifies null, empty, NaN, and other missing value patterns

### 📁 **Partitioned Output**
- **Flag-Based Partitioning**: Reports organized by quality flags
- **Parquet Format**: Efficient storage and querying
- **HDFS Storage**: Distributed storage in data lake

## Files

### `data_quality_report.py`
Main job that performs comprehensive data quality assessment.

**Features:**
- Reads all data sources from Bronze Layer
- Calculates missing value percentages
- Assigns quality flags based on thresholds
- Generates partitioned reports in HDFS

**Output Structure:**
```
/opt/spark/data/bronze_layer/data_quality/data_quality_report/
├── flag=Green/           # High quality data (0-10% missing)
├── flag=Yellow/          # Medium quality data (10-30% missing)
└── flag=Red/             # Low quality data (>30% missing)
```

## Usage

### Run Data Quality Report
```bash
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/data_quality/data_quality_report.py
```

### Run as Part of Pipeline
The data quality report is automatically executed as the first step in the main pipeline:
```bash
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/run_pipeline_1.py
```

## Report Schema

| Column | Type | Description |
|--------|------|-------------|
| `datasource` | String | Name of the data file |
| `column` | String | Column name being analyzed |
| `null_count` | Integer | Number of missing values |
| `count` | Integer | Total number of rows |
| `null_percentage` | Double | Percentage of missing values |
| `flag` | String | Quality flag (Green/Yellow/Red) |

## Quality Thresholds

| Flag | Missing Values | Description |
|------|----------------|-------------|
| 🟢 Green | 0-10% | High quality data, ready for processing |
| 🟡 Yellow | 10-30% | Medium quality, may need attention |
| 🔴 Red | >30% | Low quality, requires investigation |

## Integration

### Pipeline Integration
- **First Step**: Data quality assessment runs before all transformations
- **Quality Monitoring**: Continuous monitoring of data health
- **Proactive Detection**: Early identification of data issues

### Data Lake Architecture
- **Bronze Layer**: Raw data quality assessment
- **Quality Reports**: Stored in dedicated quality layer
- **Flag Partitioning**: Enables efficient querying by quality level

## Benefits

### 📈 **Proactive Monitoring**
- Early detection of data quality issues
- Automated flagging of problematic datasets
- Continuous quality assessment

### 🎯 **Efficient Analysis**
- Partitioned reports for quick access
- Flag-based filtering for targeted analysis
- Comprehensive coverage of all data sources

### 🔧 **Operational Excellence**
- Reduced manual quality checks
- Standardized quality metrics
- Scalable quality monitoring

## Technical Details

### Missing Value Detection
The system identifies various types of missing values:
- `NULL` values
- Empty strings (`""`)
- Whitespace-only strings
- `"nan"`, `"null"`, `"NULL"`, `"None"`
- `"N/A"`, `"n/a"`
- Empty arrays (`[]`)
- Empty objects (`{}`)
- `"undefined"`, `"UNDEFINED"`

### Array Column Handling
Special logic for array-type columns:
- Checks for `NULL` arrays
- Validates empty arrays using `size()` function
- Prevents data type mismatch errors

### Performance Optimization
- **Caching**: DataFrames cached for repeated access
- **Partitioning**: Output partitioned by quality flags
- **Parquet Format**: Efficient columnar storage
- **Distributed Processing**: YARN cluster execution
