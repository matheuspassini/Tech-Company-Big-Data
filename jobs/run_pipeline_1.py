#!/usr/bin/env python3

import subprocess
import os
import sys
import zipfile

# Add utils to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.config import SPARK_CONFIGS

def create_utils_zip():
    """Create utils.zip file with all utility modules and upload to HDFS"""
    utils_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'utils')
    utils_zip_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'utils.zip')
    hdfs_utils_zip_path = "hdfs://master:8080/utils.zip"
    
    print("Creating utils.zip for shared utilities...")
    
    with zipfile.ZipFile(utils_zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(utils_dir):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    # Add files with utils/ prefix to maintain module structure
                    arc_name = os.path.relpath(file_path, os.path.dirname(utils_dir))
                    zf.write(file_path, arc_name)
                    print(f"   Added: {arc_name}")
    
    print(f"utils.zip created locally at: {utils_zip_path}")
    
    # Upload to HDFS
    print("Uploading utils.zip to HDFS...")
    import subprocess
    result = subprocess.run(['hdfs', 'dfs', '-put', '-f', utils_zip_path, hdfs_utils_zip_path], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"utils.zip uploaded successfully to HDFS: {hdfs_utils_zip_path}")
    else:
        print(f"Failed to upload utils.zip to HDFS: {result.stderr}")
        raise Exception("Failed to upload utils.zip to HDFS")
    
    return hdfs_utils_zip_path

# Jobs usando utilitários genéricos
# Ordenados por volume de dados (mais pesados primeiro)
jobs = [
    ("Salary History", "/opt/spark/apps/bronze_to_silver/salary_history_silver_layer.py"), 
    ("Tasks", "/opt/spark/apps/bronze_to_silver/tasks_silver_layer.py"),                    
    ("Employees", "/opt/spark/apps/bronze_to_silver/employees_silver_layer.py"),           
    ("Projects", "/opt/spark/apps/bronze_to_silver/projects_silver_layer.py"),              
    ("Clients", "/opt/spark/apps/bronze_to_silver/clients_silver_layer.py"),               
    ("Departments", "/opt/spark/apps/bronze_to_silver/departments_silver_layer.py"),        
    ("Data Quality Report", "/opt/spark/apps/data_quality/data_quality_report.py")         
]

failed_jobs = {}

print("=" * 80)
print("PIPELINE 1 - BRONZE TO SILVER ETL JOBS")
print("=" * 80)
print("Using shared utilities and centralized configurations")
print("Jobs ordered by data volume (heaviest first) for optimal resource usage")
print("=" * 80)

# Create utils.zip before running jobs
utils_zip_path = create_utils_zip()

for i, (job_name, job_file) in enumerate(jobs, 1):
    print(f"\n[{i}/{len(jobs)}] Running {job_name}")
    print(f"   File: {job_file}")
    print(f"   App Name: {SPARK_CONFIGS.get(job_name.lower().replace(' ', '_'), 'Unknown')}")
    
    try:
        # Execute with utils.zip for shared utilities
        result = subprocess.run([
            'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'cluster',
            '--py-files', utils_zip_path,
            job_file
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"   SUCCESS: {job_name} completed successfully")
        else:
            print(f"   FAILED: {job_name} failed with exit code {result.returncode}")
            print(f"   Error: {result.stderr}")
            failed_jobs[job_name] = {
                "file": job_file,
                "error": f"Exit code: {result.returncode}",
                "stderr": result.stderr
            }
            
    except Exception as e:
        print(f"   FAILED: {job_name} failed with exception: {e}")
        failed_jobs[job_name] = {
            "file": job_file,
            "error": str(e)
        }

print("\n" + "=" * 80)
print("PIPELINE COMPLETED")
print("=" * 80)

if failed_jobs:
    print(f"\nFAILED: {len(failed_jobs)} jobs failed:")
    for job_name, details in failed_jobs.items():
        print(f"   - {job_name}: {details['error']}")
        if 'stderr' in details:
            print(f"     Details: {details['stderr'][:200]}...")
else:
    print(f"\nSUCCESS: All {len(jobs)} jobs completed successfully!")

print("=" * 80) 