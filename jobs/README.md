# Jobs Directory

This directory contains all Spark applications for the data pipeline. All jobs are designed to run in **cluster mode** with YARN for distributed processing.

## Structure

```
jobs/
├── bronze_to_silver/         # Bronze to Silver transformations
│   ├── employees_silver_layer.py
│   ├── departments_silver_layer.py
│   ├── clients_silver_layer.py
│   ├── tasks_silver_layer.py
│   └── salary_history_silver_layer.py
├── silver_to_gold/           # Silver to Gold transformations
│   └── department_analytics_gold.py
└── run_pipeline_1.py        # Main pipeline execution script
```

## Bronze to Silver Layer

### Self-contained Transformation Functions
Each job file contains all necessary transformation functions for data quality:

- **clean_null_dates()**: Handle null dates with default values ("0000-01-01")
- **clean_null_numbers()**: Handle null numeric values with default (0)
- **clean_null_floats()**: Handle null float values with default (0.0)
- **clean_null_strings()**: Handle null string values with default ("Unknown")
- **clean_null_booleans()**: Handle null boolean values with default (False)
- **clean_arrays()**: Handle null array values with default text

### Silver Layer Jobs
Each job processes one entity from bronze to silver in cluster mode:

- **employees_silver_layer.py**: Process employee data
- **departments_silver_layer.py**: Process department data
- **clients_silver_layer.py**: Process client data
- **tasks_silver_layer.py**: Process task data
- **salary_history_silver_layer.py**: Process salary history data

## Gold Layer Jobs
The Gold layer transforms cleaned data into business intelligence insights:

- **department_analytics_gold.py**: Creates comprehensive department analytics with aggregated metrics

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

## Execution

### Individual Jobs (Cluster Mode)
```bash
# Bronze to Silver Jobs
# Employees job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/bronze_to_silver/employees_silver_layer.py

# Departments job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/bronze_to_silver/departments_silver_layer.py

# Clients job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/bronze_to_silver/clients_silver_layer.py

# Tasks job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/bronze_to_silver/tasks_silver_layer.py

# Salary History job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/bronze_to_silver/salary_history_silver_layer.py

# Silver to Gold Jobs
# Department Analytics job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/silver_to_gold/department_analytics_gold.py
```

### Full Pipeline (Cluster Mode)
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline_1.py
```

## Cluster Mode Features

- **Self-contained Jobs**: No external dependencies or imports
- **Distributed Processing**: Driver runs in separate container managed by YARN
- **Resource Management**: Dynamic allocation via YARN
- **Fault Tolerance**: Automatic recovery from failures
- **Monitoring**: Real-time tracking via YARN Web UI

## Job Monitoring

### View Active Applications
```bash
docker exec tech-data-lake-master yarn application -list
```

### View Job Logs
```bash
docker exec tech-data-lake-master yarn logs -applicationId <application_id>
```

### Web Interfaces
- **YARN Web UI**: http://localhost:8081
- **Spark History Server**: http://localhost:18081

## Features

- **Partitioning**: Data partitioned by year/month/day (Silver) and region/department/hire_year (Gold)
- **Data Quality**: Null value handling with defaults
- **Error Handling**: Comprehensive error tracking
- **Self-contained Design**: All transformation functions embedded in job files
- **Cluster Mode**: Distributed processing with YARN
- **Scalability**: Easy to add/remove worker nodes
- **Business Intelligence**: Advanced analytics and insights in Gold layer
- **Data Quality Monitoring**: Automatic detection and flagging of data issues 