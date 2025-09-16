# Jobs Directory

This directory contains all Spark applications for the data pipeline. All jobs are designed to run in **cluster mode** with YARN for distributed processing and use shared utilities for consistent operations.

## Structure

```
jobs/
├── bronze_to_silver/         # Bronze to Silver transformations
│   ├── employees_silver_layer.py
│   ├── departments_silver_layer.py
│   ├── clients_silver_layer.py
│   ├── tasks_silver_layer.py
│   ├── salary_history_silver_layer.py
│   ├── projects_silver_layer.py
│   └── README.md
├── silver_to_gold/           # Silver to Gold transformations
│   ├── department_analytics_gold.py
│   └── README.md
├── data_quality/             # Data Quality Assessment
│   ├── data_quality_report.py
│   └── README.md
├── utils/                    # Shared utilities
│   ├── __init__.py
│   ├── config.py
│   ├── spark_utils.py
│   └── README.md
├── run_pipeline.py          # Main pipeline execution script
├── utils.zip                # Utilities archive for distribution
└── README.md                # This file
```

## Shared Utilities

All jobs use centralized utilities for consistent operations:

- **config.py**: Centralized Spark configurations and paths
- **spark_utils.py**: Common data processing and logging functions
- **utils.zip**: Automatically created and distributed to all containers

## Bronze to Silver Layer

### Shared Transformation Functions
All jobs use centralized transformation functions from shared utilities:

- **clean_null_dates()**: Handle null dates with default values ("0000-01-01")
- **clean_null_numbers()**: Handle null numeric values with default (0)
- **clean_null_floats()**: Handle null float values with default (0.0)
- **clean_null_strings()**: Handle null string values with default ("Unknown")
- **clean_null_booleans()**: Handle null boolean values with default (False)
- **clean_arrays()**: Handle null array values with default text

### Logging System
All transformation functions use centralized logging with `@log_execution` decorator:

- **Execution Timing**: Automatic measurement of function execution time
- **Record Counting**: Automatic counting of records processed by each function
- **Structured Logs**: Consistent log format with timestamps and log levels
- **Performance Monitoring**: Track execution time and data processing metrics

### Silver Layer Jobs
Each job processes one entity from bronze to silver in cluster mode:

- **employees_silver_layer.py**: Process employee data
- **departments_silver_layer.py**: Process department data
- **clients_silver_layer.py**: Process client data
- **tasks_silver_layer.py**: Process task data
- **salary_history_silver_layer.py**: Process salary history data
- **projects_silver_layer.py**: Process project data

## Gold Layer Jobs
The Gold layer transforms cleaned data into business intelligence insights:

- **department_analytics_gold.py**: Creates comprehensive department analytics with aggregated metrics
  - **Input**: Silver layer employee and department data
  - **Output**: Parquet format (partitioned)
  - **Analytics**: 385 employees across 8 departments in 4 regions
  - **Metrics**: Budget, salary, performance, and project analytics

## Data Quality Layer Jobs
The Data Quality layer provides automated assessment and monitoring of data quality across all sources:

- **data_quality_report.py**: Analyzes data quality and generates partitioned reports by quality flags

### Department Analytics Features
- **Workforce Composition**: Headcount, positions, education levels
- **Financial Performance**: Budget allocation, salary ratios, cost analysis
- **Skills & Competencies**: Skills and certification analysis
- **Experience & Performance**: Years of experience and performance metrics
- **Project Management**: Project workload and assignment analysis
- **Work-Life Balance**: Overtime and vacation metrics
- **Data Quality Monitoring**: Automatic detection of data quality issues
- **Department Health Assessment**: Budget and performance flags

### Analytics Metrics
- Employee counts and position diversity
- Financial metrics (budget, salary ratios)
- Skills and certification analysis
- Experience and performance metrics
- Workload and project metrics
- Work-life balance indicators
- Department health flags

### Partitioning Strategy
- **Region**: Geographic distribution analysis
- **Department Name**: Department-specific insights
- **Hire Year**: Temporal analysis and trends

### Data Quality Features
- **Multi-Format Support**: Handles CSV, JSON, and Parquet files
- **Quality Assessment**: Analyzes missing values across all columns
- **Quality Flags**: 
  - **Green**: 0-10% missing values (high quality)
  - **Yellow**: 10-30% missing values (medium quality)
  - **Red**: >30% missing values (low quality)
- **Partitioned Reports**: Quality reports partitioned by flag for easy analysis
- **Comprehensive Coverage**: Analyzes all 6 data sources (departments, clients, employees, tasks, salary_history, projects)

## Execution

### Individual Jobs (Cluster Mode)
```bash
# Bronze to Silver Jobs
# Employees job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/bronze_to_silver/employees_silver_layer.py

# Departments job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/bronze_to_silver/departments_silver_layer.py

# Clients job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/bronze_to_silver/clients_silver_layer.py

# Tasks job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/bronze_to_silver/tasks_silver_layer.py

# Salary History job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/bronze_to_silver/salary_history_silver_layer.py

# Projects job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/bronze_to_silver/projects_silver_layer.py

# Silver to Gold Jobs
# Department Analytics job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/silver_to_gold/department_analytics_gold.py

# Data Quality Jobs
# Data Quality Report job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/data_quality/data_quality_report.py

### Full Pipeline (Cluster Mode)
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline.py
```

**Complete Pipeline Execution:**
- **8 Total Jobs**: 6 Bronze→Silver + 1 Silver→Gold + 1 Data Quality
- **Shared Utilities**: Centralized configuration and logging via utils.zip
- **Cluster Mode**: All jobs run in distributed mode with YARN
- **Output Generation**: Parquet analytics files
- **Processing Time**: ~15-20 minutes for complete pipeline

## Cluster Mode Features

- **Shared Utilities**: Centralized utilities distributed via utils.zip
- **Distributed Processing**: Driver runs in separate container managed by YARN
- **Resource Management**: Dynamic allocation via YARN
- **Fault Tolerance**: Automatic recovery from failures
- **Monitoring**: Real-time tracking via YARN Web UI

## Job Monitoring

### Monitor Applications in Real-time
```bash
# Monitor YARN applications with watch (real-time updates every 3 seconds)
watch -n 3 'docker exec tech-data-lake-master yarn application -list'

# Monitor applications with different intervals
watch -n 5 'docker exec tech-data-lake-master yarn application -list'  # 5 seconds
watch -n 1 'docker exec tech-data-lake-master yarn application -list'  # 1 second
```

### View Active Applications
```bash
docker exec tech-data-lake-master yarn application -list
```

### View Job Logs
```bash
# YARN logs (detailed cluster logs)
docker exec tech-data-lake-master yarn logs -applicationId <application_id>

### Log Format Example
```
2024-01-15 10:30:15,123 - __main__ - INFO - Starting execution of clean_null_dates
2024-01-15 10:30:16,456 - __main__ - INFO - Function clean_null_dates processed 1000 records
2024-01-15 10:30:17,789 - __main__ - INFO - Function clean_null_dates completed in 1.23 seconds
```

### Web Interfaces
- **YARN Web UI**: http://localhost:8081
- **Spark History Server**: http://localhost:18081

## Features

- **Partitioning**: Data partitioned by year/month/day (Silver) and region/department/hire_year (Gold)
- **Data Quality**: Null value handling with defaults
- **Error Handling**: Comprehensive error tracking
- **Shared Utilities**: Centralized utilities for consistent operations
- **Cluster Mode**: Distributed processing with YARN
- **Scalability**: Easy to add/remove worker nodes
- **Business Intelligence**: Advanced analytics and insights in Gold layer
- **Data Quality Monitoring**: Automatic detection and flagging of data issues
- **Quality Assessment**: Comprehensive data quality analysis with flag-based partitioning
- **Comprehensive Logging**: Professional logging system with decorators for observability and debugging
- **Performance Monitoring**: Automatic execution time tracking and record counting
- **Production Ready**: Enterprise-grade logging for production environments

## Project Completion Status

### **COMPLETED FEATURES:**
- **Bronze to Silver Processing**: All 6 data sources processed (employees, departments, clients, tasks, salary_history, projects)
- **Silver to Gold Analytics**: Department analytics with comprehensive business intelligence
- **Data Quality Assessment**: Automated quality monitoring across all data sources
- **Pipeline Integration**: Complete end-to-end pipeline with shared utilities
- **Output Generation**: Parquet analytics format
- **Documentation**: Comprehensive documentation and insights reports
- **Testing**: Full test suite with 79% code coverage
- **CI/CD**: Automated testing and quality assurance pipeline

### **ANALYTICS RESULTS:**
- **385 employees** processed across **8 departments** in **4 regions**
- **$491M total budget** with **$5.4M salary costs**
- **1,034 projects** tracked with performance metrics
- **Department rankings** and **regional analysis** completed
- **Business insights** generated for strategic decision-making

### **DELIVERABLES:**
- **HDFS Analytics**: `/opt/spark/data/gold_layer/department_analytics.parquet/`
- **Insights Report**: `project_results/DEPARTMENT_ANALYTICS_INSIGHTS.md`
- **Complete Documentation**: Updated READMEs across all components

### **DATA QUALITY CONCERNS:**
**CRITICAL FINDING**: The pipeline processing revealed significant data quality issues:

- **Incomplete Records**: 615 out of 1,000 initial employee records were filtered out due to data quality issues
- **Missing Referential Data**: Employee records with invalid department references were excluded
- **Data Inconsistencies**: Performance scores and budget data show anomalies requiring source validation
- **Quality Flags**: Data quality assessment identified Red and Yellow flags across multiple datasets

**Business Impact**: These quality issues may affect the reliability of analytics and business decisions. The company should review and improve data collection processes at the source systems. 