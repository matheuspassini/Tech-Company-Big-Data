# Jobs Directory

This directory contains all Spark applications for the data pipeline.

## Structure

```
jobs/
├── bronze_to_silver/         # Bronze to Silver transformations
│   ├── transformations/      # Modular transformation functions
│   │   ├── employees_transformations.py
│   │   ├── departments_transformations.py
│   │   ├── clients_transformations.py
│   │   ├── tasks_transformations.py
│   │   └── salary_history_transformations.py
│   ├── employees_silver_layer.py
│   ├── departments_silver_layer.py
│   ├── clients_silver_layer.py
│   ├── tasks_silver_layer.py
│   └── salary_history_silver_layer.py
├── silver_to_gold/           # Silver to Gold transformations (future)
└── run_pipeline_1.py        # Main pipeline execution script
```

## Bronze to Silver Layer

### Transformation Functions
Located in `bronze_to_silver/transformations/`, these modular functions handle data quality:

- **clean_null_dates()**: Handle null dates with default values ("0000-01-01")
- **clean_null_numbers()**: Handle null numeric values with default (0)
- **clean_null_floats()**: Handle null float values with default (0.0)
- **clean_null_strings()**: Handle null string values with default ("Unknown")
- **clean_null_booleans()**: Handle null boolean values with default (False)
- **clean_arrays()**: Handle null array values with default text

### Silver Layer Jobs
Each job processes one entity from bronze to silver:

- **employees_silver_layer.py**: Process employee data
- **departments_silver_layer.py**: Process department data
- **clients_silver_layer.py**: Process client data
- **tasks_silver_layer.py**: Process task data
- **salary_history_silver_layer.py**: Process salary history data

## Execution

### Individual Jobs
```bash
docker exec tech-data-lake-master spark-submit --deploy-mode client /opt/spark/jobs/bronze_to_silver/employees_silver_layer.py
```

### Full Pipeline
```bash
docker exec tech-data-lake-master python3 /opt/spark/apps/run_pipeline_1.py
```

## Features

- **Partitioning**: Data partitioned by year/month/day
- **Data Quality**: Null value handling with defaults
- **Error Handling**: Comprehensive error tracking
- **Modular Design**: Reusable transformation functions 