# Tech Data Lake

This project implements a Data Lake using Apache Spark and Hadoop, configured with Docker for easy development and deployment.

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
│   │   ├── transformations/      # Modular transformation functions
│   │   │   ├── employees_transformations.py
│   │   │   ├── departments_transformations.py
│   │   │   ├── clients_transformations.py
│   │   │   ├── tasks_transformations.py
│   │   │   └── salary_history_transformations.py
│   │   ├── employees_silver_layer.py
│   │   ├── departments_silver_layer.py
│   │   ├── clients_silver_layer.py
│   │   ├── tasks_silver_layer.py
│   │   └── salary_history_silver_layer.py
│   ├── silver_to_gold/           # Silver to Gold transformations
│   └── run_pipeline_1.py        # Main pipeline execution script
├── yarn/                         # Hadoop/YARN configurations
├── ssh/                          # SSH configurations for node communication
├── requirements/                  # Python dependencies
├── test/                         # Test files
├── Dockerfile                    # Docker image configuration
├── docker-compose.yml           # Service configuration
├── entrypoint.sh                # Container startup script
├── convert_parquet_to_csv.py    # Utility script
└── .env.data-lake              # Environment variables
```

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

### Data Quality Handling:
- **Year 0 Partition**: Contains records with null or invalid dates
- **Default Values**: Null dates are set to "0000-01-01" and partitioned as year=0
- **Data Integrity**: Allows identification and processing of problematic records
- **Audit Trail**: Maintains original data while flagging quality issues

### Key Features:
- **Partitioning**: Data is partitioned by year/month/day for optimal query performance
- **Data Quality**: Null values are handled with appropriate defaults
- **Transformations**: Modular transformation functions for each data type
- **Format**: Parquet format for efficient storage and querying

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

3. Run the Python pipeline:
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline_1.py
```

4. Access the web interfaces:
- **YARN Web UI**: http://localhost:8081
- **Spark History Server**: http://localhost:18081