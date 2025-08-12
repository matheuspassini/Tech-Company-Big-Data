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
- **Partitioning**: By region, department name, and hire year

### Business Questions Answered

#### 1. Workforce Composition & Diversity
- Total headcount per department and region
- Number of different positions in each department
- Distribution of education levels across departments
- Maximum education level achieved in each department

#### 2. Financial Performance & Budget Management
- Total budget allocation per department
- Average budget per employee in each department
- Percentage of department budget spent on salaries
- Department health flags (Red/Green/Yellow)

#### 3. Employee Skills & Competencies
- Average number of skills per employee
- Total number of skills across all employees
- Average number of certifications per employee
- Total number of certifications per department

#### 4. Experience & Performance Analysis
- Average years of experience per department
- Range of experience (min/max) in each department
- Average performance score per department
- Performance variation across departments

#### 5. Project Management & Workload
- Average projects assigned per employee
- Total number of projects per department
- Project workload variation across departments

#### 6. Work-Life Balance & Employee Wellbeing
- Average overtime hours per department
- Total overtime hours per department
- Vacation days used vs. remaining per department
- Overtime pattern analysis

#### 7. Salary Analysis & Compensation
- Average salary per department
- Salary range (min/max) in each department
- Total salary cost per department
- Salary comparison across departments and regions

#### 8. Temporal Analysis & Hiring Trends
- Department composition changes by hire year
- Hiring trends across regions and departments
- Employee experience variation by year of hire

#### 9. Regional Performance Comparison
- Department performance across different regions
- Regional differences in budget allocation, salaries, or performance
- Highest performing departments by region

#### 10. Department Health Assessment
- Departments flagged as healthy (Green Flag)
- Departments with budget concerns (Red Flag)
- Departments needing review (Yellow/Gray Flag)

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
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/silver_to_gold/department_analytics_gold.py
```

#### Prerequisites
- Silver layer data must be processed first
- Employee and department data must be available in Silver layer

### Output Structure

```
/opt/spark/data/gold_layer/department_analytics.parquet/
├── region=Central/
│   ├── department_name=Engineering/
│   │   ├── hire_year=2020/
│   │   ├── hire_year=2021/
│   │   └── hire_year=2022/
│   └── department_name=Marketing/
│       ├── hire_year=2021/
│       └── hire_year=2022/
└── region=South/
    ├── department_name=Sales/
    │   ├── hire_year=2020/
    │   └── hire_year=2021/
    └── department_name=DevOps/
        └── hire_year=2022/
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
- **Partitioning**: Optimized for query performance by region, department, and hire year
- **Cluster Mode**: Distributed processing with YARN resource management 