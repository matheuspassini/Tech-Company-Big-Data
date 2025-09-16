#!/usr/bin/env python3

from typing import Dict, List

# HDFS paths
HDFS_BASE_PATH = "hdfs://master:8080/opt/spark/data"
BRONZE_LAYER_PATH = f"{HDFS_BASE_PATH}/bronze_layer"
SILVER_LAYER_PATH = f"{HDFS_BASE_PATH}/silver_layer/"
GOLD_LAYER_PATH = f"{HDFS_BASE_PATH}/gold_layer/"

# Data quality thresholds
QUALITY_THRESHOLDS = {
    "red": 30.0,
    "yellow": 10.0,
    "green": 0.0
}

# File paths for data quality
DATA_QUALITY_FILES = [
    f"{BRONZE_LAYER_PATH}/departments.csv",
    f"{BRONZE_LAYER_PATH}/clients.csv", 
    f"{BRONZE_LAYER_PATH}/employees.json",
    f"{BRONZE_LAYER_PATH}/tasks.json",
    f"{BRONZE_LAYER_PATH}/salary_history.parquet",
    f"{BRONZE_LAYER_PATH}/projects.parquet"
]

# Spark configurations
SPARK_CONFIGS = {
    "data_quality": "Source-Data-Quality-Application-Cluster",
    "departments": "Departments-Tech-Company-Application-Cluster",
    "employees": "Employees-Tech-Company-Application-Cluster",
    "salary_history": "Salary-History-Tech-Company-Application-Cluster",
    "clients": "Clients-Tech-Company-Application-Cluster",
    "tasks": "Tasks-Tech-Company-Application-Cluster",
    "projects": "Projects-Tech-Company-Application-Cluster",
    "department_analytics": "Department-Analytics-Gold-Layer-Application-Cluster"
}

# Input paths
INPUT_PATHS = {
    "departments": f"{BRONZE_LAYER_PATH}/departments.csv",
    "employees": f"{BRONZE_LAYER_PATH}/employees.json",
    "salary_history": f"{BRONZE_LAYER_PATH}/salary_history.parquet",
    "clients": f"{BRONZE_LAYER_PATH}/clients.csv",
    "tasks": f"{BRONZE_LAYER_PATH}/tasks.json",
    "projects": f"{BRONZE_LAYER_PATH}/projects.parquet",
    "department_analytics_employees": f"{SILVER_LAYER_PATH}employee.parquet",
    "department_analytics_departments": f"{SILVER_LAYER_PATH}departments.parquet"
}

# Output paths
OUTPUT_PATHS = {
    "data_quality": f"{BRONZE_LAYER_PATH}/data_quality/data_quality_report",
    "departments": f"{SILVER_LAYER_PATH}departments.parquet",
    "employees": f"{SILVER_LAYER_PATH}employee.parquet",
    "salary_history": f"{SILVER_LAYER_PATH}salary_history.parquet",
    "clients": f"{SILVER_LAYER_PATH}clients.parquet",
    "tasks": f"{SILVER_LAYER_PATH}tasks.parquet",
    "projects": f"{SILVER_LAYER_PATH}projects.parquet",
    "department_analytics": f"{GOLD_LAYER_PATH}department_analytics.parquet"
}

# Data cleaning rules for different tables
CLEANING_RULES = {
    "departments": {
        "null_dates": ["founded_date", "last_audit_date"],
        "null_floats": ["budget", "quarterly_budget", "yearly_revenue"],
        "null_integers": ["headcount", "office_size"],
        "null_strings": ["location", "region", "description"],
        "arrays": ["tech_stack"]
    },
    "employees": {
        "null_dates": ["hire_date", "last_review_date", "next_review_date"],
        "null_floats": ["performance_score", "attendance_rate"],
        "null_integers": ["projects_assigned", "overtime_hours", "vacation_days_used", "vacation_days_remaining", "years_experience", "training_hours", "sick_days"],
        "null_strings": ["position", "work_location"],
        "null_booleans": ["is_manager"],
        "arrays": ["skills", "certifications"]
    },
    "salary_history": {
        "null_dates": ["effective_date"],
        "null_floats": ["salary", "bonus_amount", "stock_options"],
        "null_strings": ["change_reason", "position", "currency"]
    },
    "clients": {
        "null_dates": ["contract_start_date", "last_contact_date", "customer_since"],
        "null_integers": ["contract_value", "annual_revenue", "employee_count"],
        "null_floats": ["churn_risk_score", "satisfaction_score", "upsell_potential"],
        "null_strings": ["company_name", "industry", "contact_person", "email", "phone", "address", "country", "status", "payment_history", "contract_type"]
    },
    "tasks": {
        "null_dates": ["created_date", "due_date"],
        "null_integers": ["estimated_hours", "actual_hours", "dependencies"],
        "null_strings": ["name", "description", "status", "priority"],
        "arrays": ["tags"]
    },
    "projects": {
        "null_dates": ["start_date", "end_date", "estimated_completion_date"],
        "null_floats": ["budget", "actual_cost", "quality_score", "resource_utilization"],
        "null_integers": ["team_size", "milestones_completed", "total_milestones", "dependencies"],
        "null_strings": ["name", "status", "priority", "description", "risk_level", "success_metrics", "project_type", "complexity", "customer_feedback"],
        "arrays": ["technologies"]
    }
}

# Columns to drop for each table
COLUMNS_TO_DROP = {
    "departments": [],  # No columns to drop for departments
    "employees": [
        'emergency_contact', 'email', 'name', "phone", "address", 
        "emergency_phone", 'sick_days', 'attendance_rate', 'training_hours'
    ],
    "salary_history": [],  # No columns to drop for salary_history
    "clients": [],  # No columns to drop for clients
    "tasks": [],  # No columns to drop for tasks
    "projects": [],  # No columns to drop for projects
}

# Referential integrity constraints
REFERENTIAL_INTEGRITY = {
    "departments": ["id", "manager_id"],
    "employees": ["id", "department_id", "employment_type", "education", "hire_date"],
    "salary_history": ["id", "employee_id", "department_id"],
    "clients": ["id", "account_manager_id"],
    "tasks": ["id", "project_id", "assigned_to"],
    "projects": ['id', 'department_id', 'client_id']
}
