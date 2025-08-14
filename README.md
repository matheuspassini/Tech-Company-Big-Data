# Tech Data Lake

This project implements a Data Lake using Apache Spark and Hadoop, configured with Docker for easy development and deployment. All jobs run in **cluster mode** with YARN for distributed processing.

## Architecture

The project consists of the following components:

- **Master Node**: Manages the Spark cluster and coordinates operations
- **History Server**: Provides web interface for job monitoring
- **Worker Nodes**: Execute distributed tasks (scalable as needed)

## Requirements

- Docker
- Docker Compose
- Git

## Project Structure

```
project/
├── data/                          # Directory for data storage
├── jobs/                          # Spark applications
│   ├── bronze_to_silver/         # Bronze to Silver transformations
│   │   ├── employees_silver_layer.py
│   │   ├── departments_silver_layer.py
│   │   ├── clients_silver_layer.py
│   │   ├── tasks_silver_layer.py
│   │   └── salary_history_silver_layer.py
│   ├── silver_to_gold/           # Silver to Gold transformations
│   │   └── department_analytics_gold.py
│   ├── data_quality/             # Data Quality Assessment
│   │   └── data_quality_report.py
│   └── run_pipeline_1.py        # Main pipeline execution script
├── yarn/                         # Hadoop/YARN configurations
├── ssh/                          # SSH configurations for node communication
├── requirements/                  # Python dependencies
├── test/                         # Test files
├── Dockerfile                    # Docker image configuration
├── docker-compose.yml           # Service configuration
├── entrypoint.sh                # Container startup script
└── .env.data-lake              # Environment variables
```

## Cluster Mode Implementation

All Spark jobs are designed to run in **cluster mode** with the following characteristics:

- **Self-contained Jobs**: Each job file contains all necessary transformation functions
- **No External Dependencies**: Eliminates import issues in distributed environments
- **YARN Resource Management**: Efficient resource allocation and monitoring
- **Distributed Processing**: Driver runs in separate container managed by YARN

## HDFS Data Structure

The data lake is organized in layers following the medallion architecture:

### Bronze Layer (Raw Data)
```
/opt/spark/data/bronze_layer/
├── employees.json
├── departments.csv
├── clients.csv
├── tasks.json
└── salary_history.parquet
```

### Silver Layer (Transformed & Partitioned Data)
```
/opt/spark/data/silver_layer/
├── employee.parquet/
│   ├── year=0/                   # Filtered data (null dates)
│   ├── year=2024/
│   │   ├── month=1/
│   │   │   └── day=1/
│   │   └── month=2/
│   └── year=2025/
├── departments.parquet/
│   ├── year=0/                   # Filtered data (null dates)
│   ├── year=2024/
│   └── year=2025/
├── clients.parquet/
│   ├── year=0/                   # Filtered data (null dates)
│   ├── year=2024/
│   └── year=2025/
├── tasks.parquet/
│   ├── year=0/                   # Filtered data (null dates)
│   ├── year=2024/
│   │   ├── month=8/
│   │   │   ├── day=1/
│   │   │   ├── day=2/
│   │   │   └── ...
│   │   └── month=9/
│   └── year=2025/
│       ├── month=1/
│       ├── month=2/
│       ├── month=3/
│       ├── month=4/
│       └── month=5/
└── salary_history.parquet/
    ├── year=0/                   # Filtered data (null dates)
    ├── year=2024/
    └── year=2025/
```

### Gold Layer (Business Intelligence & Analytics)
```
/opt/spark/data/gold_layer/
├── department_analytics.parquet/
│   ├── region=Central/
│   │   ├── department_name=Engineering/
│   │   │   ├── hire_year=2020/
│   │   │   ├── hire_year=2021/
│   │   │   └── hire_year=2022/
│   │   └── department_name=Marketing/
│   └── region=South/
│       ├── department_name=Sales/
│       └── department_name=DevOps/
```

### Data Quality Layer (Quality Assessment & Monitoring)
```
/opt/spark/data/bronze_layer/data_quality/
├── data_quality_report/
│   ├── _SUCCESS
│   ├── flag=Green/           # High quality data (0-10% missing values)
│   ├── flag=Yellow/          # Medium quality data (10-30% missing values)
│   └── flag=Red/             # Low quality data (>30% missing values)
```

### Data Quality Handling:
- **Year 0 Partition**: Contains records with null or invalid dates
- **Default Values**: Null dates are set to "0000-01-01" and partitioned as year=0
- **Data Integrity**: Allows identification and processing of problematic records
- **Audit Trail**: Maintains original data while flagging quality issues

### Key Features:
- **Partitioning**: Data is partitioned by year/month/day and by region/department/hire year for optimal query performance
- **Data Quality**: Null values are handled with appropriate defaults
- **Transformations**: Self-contained transformation functions in each job
- **Format**: Parquet format for efficient storage and querying
- **Cluster Mode**: All jobs run in distributed mode with YARN
- **Business Intelligence**: Gold layer provides aggregated analytics and insights
- **Quality Monitoring**: Automated data quality assessment with flag-based partitioning

### Data Quality System:
- **Quality Assessment**: Analyzes missing values across all columns in all data sources
- **Quality Flags**: 
  - **Green**: 0-10% missing values (high quality)
  - **Yellow**: 10-30% missing values (medium quality)
  - **Red**: >30% missing values (low quality)
- **Partitioned Reports**: Quality reports are partitioned by flag for easy analysis
- **Multi-Format Support**: Handles CSV, JSON, and Parquet files
- **Comprehensive Coverage**: Analyzes all 6 data sources (departments, clients, employees, tasks, salary_history, projects)

## How to Use

1. Clone the repository:
```bash
git clone https://github.com/matheuspassini/Tech-Company-Big-Data.git
cd projeto3
```

2. Start the cluster with the desired number of workers:
```bash
docker-compose -p tech-data-lake -f docker-compose.yml up -d --scale worker=3
```

3. Run the complete pipeline (cluster mode):
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline_1.py
```

4. Run individual jobs (cluster mode):
```bash
# Silver Jobs
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

# Gold Jobs
# Department Analytics job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/silver_to_gold/department_analytics_gold.py

# Data Quality Jobs
# Data Quality Report job
docker exec tech-data-lake-master spark-submit --master yarn --deploy-mode cluster /opt/spark/apps/data_quality/data_quality_report.py

5. Monitor jobs and access web interfaces:
- **YARN Web UI**: http://localhost:8081
- **Spark History Server**: http://localhost:18081

6. View job logs:
```bash
# List applications
docker exec tech-data-lake-master yarn application -list

# View logs for specific application
docker exec tech-data-lake-master yarn logs -applicationId <application_id>
```

## Job Execution Details

- **Deploy Mode**: All jobs run in `cluster` mode
- **Master**: YARN resource manager
- **Driver**: Runs in separate container managed by YARN
- **Executors**: Distributed across worker nodes
- **Logs**: Available through YARN logs command
- **Monitoring**: Real-time tracking via YARN Web UI

## Performance Benefits

- **Distributed Processing**: Workload spread across multiple nodes
- **Resource Optimization**: Dynamic resource allocation via YARN
- **Scalability**: Easy to add/remove worker nodes
- **Fault Tolerance**: Automatic recovery from node failures
- **Monitoring**: Comprehensive job tracking and metrics
- **Business Intelligence**: Advanced analytics and insights in Gold layer