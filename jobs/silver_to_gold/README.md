# Silver to Gold Layer - Business Intelligence

This directory contains Spark applications that transform cleaned data from the Silver layer into business intelligence insights in the Gold layer.

## Overview

The Gold layer represents the final stage of the data lake architecture, where cleaned and validated data is transformed into actionable business intelligence and analytics.

## Structure

```
silver_to_gold/
├── department_analytics_gold.py    # Department analytics and insights
└── README.md                       # This file
```

## Department Analytics Gold Job

### Purpose
Creates comprehensive department analytics by aggregating employee and department data to answer critical business questions about organizational performance, resource allocation, and workforce management.

### Input Data
- **Silver Layer**: Cleaned employee and department data
- **Source Tables**: 
  - `employee.parquet` - Processed employee data
  - `departments.parquet` - Processed department data

### Output Data
- **Gold Layer**: `department_analytics.parquet`
- **Format**: Partitioned Parquet files
- **Partitioning**: By region and department name


### Key Features

#### Data Quality Monitoring
- **Automatic Detection**: Identifies "Unknown" values in position and education data
- **Quality Flags**: Marks departments with data quality issues
- **Review Prioritization**: Helps prioritize data cleaning efforts

#### Department Health Assessment
- **Green Flag**: Departments operating within budget constraints
- **Red Flag**: Departments with salary costs exceeding budget
- **Yellow Flag**: Departments at budget threshold
- **Gray Flag**: Departments requiring review

#### Advanced Analytics
- **Unique Position Sets**: Collects unique positions per department (no duplicates)
- **Education Level Analysis**: Converts education to numeric hierarchy for analysis
- **Salary-Budget Ratios**: Calculates percentage of budget spent on salaries
- **Performance Metrics**: Aggregated performance and workload indicators

### Execution

#### Individual Job (Cluster Mode)
```bash
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster --py-files /opt/spark/apps/utils.zip /opt/spark/apps/silver_to_gold/department_analytics_gold.py
```

#### Pipeline Integration
```bash
# Run as part of complete pipeline
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline.py
```

#### Prerequisites
- Silver layer data must be processed first
- Employee and department data must be available in Silver layer

### Output Structure

```
/opt/spark/data/gold_layer/department_analytics.parquet/
├── region=Central/
│   ├── department_name=Engineering/
│   └── department_name=Marketing/
├── region=South/
│   ├── department_name=Sales/
│   ├── department_name=Customer Success/
│   ├── department_name=Product/
│   └── department_name=DevOps/
├── region=West/
│   └── department_name=HR/
└── region=East/
    └── department_name=Data Science/
```

### Business Impact

This analytics pipeline enables data-driven decision making for:
- **Resource Allocation**: Budget planning and optimization
- **Workforce Development**: Training and skill development programs
- **Performance Management**: Improvement initiatives and benchmarking
- **Regional Expansion**: Department scaling and expansion decisions
- **Employee Retention**: Satisfaction and wellbeing strategies
- **Data Quality**: Identification and resolution of data issues

### Technical Implementation

- **Data Source**: Silver layer (processed employee and department data)
- **Processing**: PySpark aggregations and transformations
- **Output**: Partitioned Parquet files in Gold layer
- **Utilities**: Uses centralized utilities from utils.zip
- **Partitioning**: Optimized for query performance by region and department
- **Cluster Mode**: Distributed processing with YARN resource management

## Project Completion Results

### **IMPLEMENTATION COMPLETED:**
- **Data Processing**: Successfully processes 385 employees across 8 departments in 4 regions
- **Analytics Generation**: Comprehensive business intelligence metrics and insights
- **Output Format**: Parquet analytics format generated
- **Pipeline Integration**: Fully integrated into complete ETL pipeline
- **Documentation**: Complete documentation and insights reports generated

### **ANALYTICS METRICS GENERATED:**
- **Financial Metrics**: $491M total budget, $5.4M salary costs, budget per employee ratios
- **Performance Metrics**: Department performance scores (2.52-2.98 range)
- **Operational Metrics**: 1,034 total projects, project distribution by department
- **Regional Analysis**: Geographic distribution and performance comparison
- **Department Rankings**: Performance-based department rankings and insights

### **DELIVERABLES:**
- **HDFS Analytics**: `/opt/spark/data/gold_layer/department_analytics.parquet/`
- **Insights Report**: `project_results/DEPARTMENT_ANALYTICS_INSIGHTS.md`

### **KEY INSIGHTS GENERATED:**
- **Marketing** leads with highest performance (2.98) and most efficient resource utilization
- **South region** dominates with 54.5% of employees and 59% of budget
- **Data Science** has highest investment per employee ($1.87M)
- **Sales** commands largest budget ($110M) but shows lowest performance (2.52)
- **Customer Success** offers highest average salary ($18,220.18)

### **DATA QUALITY LIMITATIONS:**
**IMPORTANT NOTICE**: The analytics results are based on cleaned and validated data, but several quality issues were identified:

- **Data Loss**: Only 385 out of 1,000 original employee records were processed due to quality issues
- **Source Validation Required**: Performance scores and budget allocations need verification against source systems
- **Incomplete Coverage**: Some departments may have incomplete data affecting accuracy of metrics
- **Referential Integrity**: Employee-department relationships required significant cleaning and validation

**Recommendation**: Before making strategic decisions based on these analytics, the company should validate key metrics against source systems and implement data quality improvements at the data collection level. 