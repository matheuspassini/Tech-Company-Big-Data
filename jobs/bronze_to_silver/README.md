# Bronze to Silver Layer Jobs

This directory contains Spark jobs that transform raw data from the Bronze layer into cleaned and structured data in the Silver layer. All jobs use shared utilities and run in cluster mode with YARN.

## Structure

```
bronze_to_silver/
├── employees_silver_layer.py      # Employee data transformation
├── departments_silver_layer.py    # Department data transformation
├── clients_silver_layer.py        # Client data transformation
├── tasks_silver_layer.py          # Task data transformation
├── salary_history_silver_layer.py # Salary history transformation
├── projects_silver_layer.py       # Project data transformation
└── README.md                      # This file
```

## Jobs Overview

### `employees_silver_layer.py`
Transforms employee data from JSON format to structured Parquet with data quality improvements.

**Features:**
- **Input**: `employees.json` from bronze layer
- **Output**: Partitioned Parquet files in silver layer
- **Data Quality**: Referential integrity with departments
- **Partitioning**: `year_hire_date`, `month_hire_date`, `day_hire_date`
- **Records**: ~385 valid records (from 1000 initial)

**Transformations:**
- Null value handling for all data types
- Referential integrity validation with departments
- Date partitioning for optimized queries
- Data type standardization

### `departments_silver_layer.py`
Transforms department data from CSV format to structured Parquet.

**Features:**
- **Input**: `departments.csv` from bronze layer
- **Output**: Partitioned Parquet files in silver layer
- **Data Quality**: Budget and headcount validation
- **Partitioning**: `year_founded_date`, `month_founded_date`, `day_founded_date`
- **Records**: 8 departments

**Transformations:**
- Null date handling with defaults
- Budget and financial data cleaning
- Tech stack array processing
- Geographic data standardization

### `clients_silver_layer.py`
Transforms client data from CSV format to structured Parquet.

**Features:**
- **Input**: `clients.csv` from bronze layer
- **Output**: Partitioned Parquet files in silver layer
- **Data Quality**: Client information validation
- **Partitioning**: `year_created_date`, `month_created_date`, `day_created_date`
- **Records**: Client records with contact information

**Transformations:**
- Contact information cleaning
- Industry classification standardization
- Client status validation
- Geographic data processing

### `tasks_silver_layer.py`
Transforms task data from JSON format to structured Parquet.

**Features:**
- **Input**: `tasks.json` from bronze layer
- **Output**: Partitioned Parquet files in silver layer
- **Data Quality**: Task assignment validation
- **Partitioning**: `year_created_date`, `month_created_date`, `day_created_date`
- **Records**: Task records with assignments

**Transformations:**
- Task status standardization
- Priority level validation
- Assignment data cleaning
- Date range processing

### `salary_history_silver_layer.py`
Transforms salary history data from Parquet format to structured Parquet.

**Features:**
- **Input**: `salary_history.parquet` from bronze layer
- **Output**: Partitioned Parquet files in silver layer
- **Data Quality**: Salary data validation
- **Partitioning**: `year_effective_date`, `month_effective_date`, `day_effective_date`
- **Records**: Salary history records

**Transformations:**
- Salary amount validation
- Effective date processing
- Currency standardization
- Historical data cleaning

### `projects_silver_layer.py`
Transforms project data from Parquet format to structured Parquet with comprehensive project management features.

**Features:**
- **Input**: `projects.parquet` from bronze layer
- **Output**: Partitioned Parquet files in silver layer
- **Data Quality**: Project data validation and referential integrity
- **Partitioning**: `status`, `year_start_date`, `month_start_date`, `day_start_date`
- **Records**: Project records with comprehensive metadata

**Transformations:**
- Project status standardization
- Budget and cost validation
- Technology stack array processing
- Date range validation and processing
- Referential integrity with departments and clients
- Risk level and priority standardization
- Project complexity and milestone tracking
- Quality score and resource utilization processing

## Execution

### Individual Jobs (Cluster Mode)
```bash
# Employees job
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/bronze_to_silver/employees_silver_layer.py

# Departments job
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/bronze_to_silver/departments_silver_layer.py

# Clients job
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/bronze_to_silver/clients_silver_layer.py

# Tasks job
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/bronze_to_silver/tasks_silver_layer.py

# Salary History job
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/bronze_to_silver/salary_history_silver_layer.py

# Projects job
docker exec tech-data-lake-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files /opt/spark/apps/utils.zip \
  /opt/spark/apps/bronze_to_silver/projects_silver_layer.py
```

### Full Pipeline
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline_1.py
```

## Data Quality Features

### Null Value Handling
All jobs implement consistent null value handling:
- **Dates**: Default to "0000-01-01" and partitioned as `year=0`
- **Numbers**: Default to 0
- **Floats**: Default to 0.0
- **Strings**: Default to "Unknown"
- **Booleans**: Default to False
- **Arrays**: Default to empty array

### Referential Integrity
Jobs validate referential integrity:
- **Employees**: Validated against departments
- **Tasks**: Validated against employees and projects
- **Salary History**: Validated against employees

### Data Cleaning
Standardized data cleaning operations:
- **Empty Strings**: Converted to "Unknown"
- **Invalid Dates**: Flagged and partitioned separately
- **Data Type Conversion**: Consistent type handling
- **Array Processing**: Standardized array handling

## Output Structure

### Silver Layer Organization
```
/opt/spark/data/silver_layer/
├── employees.parquet/
│   ├── year=0/                    # Invalid dates
│   ├── year=2024/
│   │   ├── month=1/
│   │   └── month=2/
│   └── year=2025/
├── departments.parquet/
│   ├── year=0/                    # Invalid dates
│   ├── year=2024/
│   └── year=2025/
├── clients.parquet/
│   ├── year=0/                    # Invalid dates
│   ├── year=2024/
│   └── year=2025/
├── tasks.parquet/
│   ├── year=0/                    # Invalid dates
│   ├── year=2024/
│   └── year=2025/
└── salary_history.parquet/
    ├── year=0/                    # Invalid dates
    ├── year=2024/
    └── year=2025/
```

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

## Shared Utilities

All jobs use centralized utilities from `utils.zip`:

### Logging
```python
from utils.spark_utils import setup_logging, log_execution

logger = setup_logging(__name__)

@log_execution
def my_transformation(df):
    # Function automatically logged with timing and record counts
    return df
```

### Data Processing
```python
from utils.spark_utils import (
    read_file, clean_dataframe, apply_referential_integrity,
    add_timestamp_column, transform_empty_strings
)

# Apply standard transformations
df = transform_empty_strings(df)
df = add_timestamp_column(df)
df = apply_referential_integrity(df, rules)
df = clean_dataframe(df, cleaning_rules)
```

### Configuration
```python
from utils.config import SPARK_CONFIGS, OUTPUT_PATHS, CLEANING_RULES

# Use centralized configurations
app_config = SPARK_CONFIGS["employees"]
output_path = OUTPUT_PATHS["employees"]
cleaning_rules = CLEANING_RULES["employees"]
```

## Performance Characteristics

### Execution Order
Jobs are executed in order of data volume (heaviest first):
1. **Salary History** (largest dataset)
2. **Tasks** (medium dataset)
3. **Employees** (medium dataset)
4. **Clients** (smaller dataset)
5. **Departments** (smallest dataset)

### Resource Usage
- **Memory**: Optimized for distributed processing
- **CPU**: Parallel processing across worker nodes
- **Storage**: Efficient Parquet format with partitioning
- **Network**: Minimal shuffle operations

### Scalability
- **Horizontal Scaling**: Add worker nodes as needed
- **Vertical Scaling**: Adjust memory and CPU per container
- **Data Partitioning**: Optimized for parallel processing
- **Resource Management**: Dynamic allocation via YARN
