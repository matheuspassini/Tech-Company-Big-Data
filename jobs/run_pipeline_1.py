#!/usr/bin/env python3

import subprocess

jobs = [
    ("Employees", "/opt/spark/apps/bronze_to_silver/employees_silver_layer.py"),
    ("Departments", "/opt/spark/apps/bronze_to_silver/departments_silver_layer.py"),
    ("Clients", "/opt/spark/apps/bronze_to_silver/clients_silver_layer.py"),
    ("Tasks", "/opt/spark/apps/bronze_to_silver/tasks_silver_layer.py"),
    ("Salary History", "/opt/spark/apps/bronze_to_silver/salary_history_silver_layer.py")
]

failed_jobs = {}

for job_name, job_file in jobs:
    print(f"Running {job_name}")
    try:
        subprocess.run(['spark-submit', '--deploy-mode', 'client', job_file])
        print("Done")
    except Exception as e:
        print(f"Error: {e}")
        failed_jobs[job_name] = {
            "file": job_file,
            "error": str(e)
        }

print("Pipeline completed")

if failed_jobs:
    print("Failed jobs:")
    for job_name, details in failed_jobs.items():
        print(f"  - {job_name}: {details['error']}")
else:
    print("All jobs completed successfully") 