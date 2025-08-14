# Tech Company Data

This directory contains synthetic data generated using the Python Faker package to simulate a technology company's data.

## Data Files

### CSV Format
- `departments.csv`: Company departments information
- `clients.csv`: Client data

### JSON Format
- `employees.json`: Employee information
- `tasks.json`: Task data

### Parquet Format
- `projects.parquet`: Project data
- `salary_history.parquet`: Employee salary history

## Data Generation

Generated using [Faker](https://pypi.org/project/Faker/) to create synthetic company data including departments, employees, projects, clients, tasks and salary records.

## Note

This is synthetic data generated for testing and development purposes. It does not represent real company data.

# Data Quality Issues Documentation

This directory contains the employees dataset with intentionally introduced data quality issues for testing purposes.

## Data Quality Problems

The dataset has been modified to include the following data quality issues:

1. **Null Values (10%)**
   - 10% of all fields in each employee record are randomly set to `null`
   - This affects all field types (strings, numbers, booleans, arrays)

2. **Empty Strings (20%)**
   - 20% of string fields in each employee record are randomly set to empty strings (`""`)
   - This only affects fields that originally contained string values

3. **Null IDs (2%)**
   - 2% of employee records have their `id` field set to `null`
   - This creates records without a unique identifier
   
## Purpose

These data quality issues are introduced to test:
- Data validation and cleaning processes
- Error handling in data processing pipelines
- Data quality monitoring systems
- Data analysis robustness
- Application behavior with missing or empty data
- Primary key integrity and referential integrity

## Impact

The introduced issues may affect:
- Data completeness
- Data consistency
- Data analysis results
- Application functionality
- Data visualization
- Reporting accuracy
- Data relationships and joins
- Data integrity constraints