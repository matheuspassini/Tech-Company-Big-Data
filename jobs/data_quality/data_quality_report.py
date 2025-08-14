#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, when, trim, lower, isnan, size

spark = SparkSession.builder.appName('Source-Data-Quality-Application-Cluster').getOrCreate()

def null_data_report():
    data_files = [
        "hdfs://master:8080/opt/spark/data/bronze_layer/departments.csv", 
        "hdfs://master:8080/opt/spark/data/bronze_layer/clients.csv",
        "hdfs://master:8080/opt/spark/data/bronze_layer/employees.json",
        "hdfs://master:8080/opt/spark/data/bronze_layer/tasks.json",
        "hdfs://master:8080/opt/spark/data/bronze_layer/salary_history.parquet",
        "hdfs://master:8080/opt/spark/data/bronze_layer/projects.parquet"
    ]
    
    quality_data_report = {
        "datasource": [],
        "column": [],
        "null_count": [],
        "count": [],
        "null_percentage": [],
        "flag": []
    }
    
    for file_path in data_files:
        file_name = file_path.split('/')[-1] 
        
        if file_path.endswith(".csv"):
            df = spark.read.option("multiline", "true").option("escape", "\"").csv(file_path, header=True)
        elif file_path.endswith(".json"):
            df = spark.read.option("multiline", "true").json(file_path)
        elif file_path.endswith(".parquet"):
            df = spark.read.parquet(file_path)

        df = df.cache()
        total_rows = df.count()
        
        for column in df.columns:
            column_type = df.schema[column].dataType
            
            # Different logic for arrays vs other types
            if str(column_type).startswith('ArrayType'):
                # For arrays, only check if null or empty array
                missing_count = df.filter(
                    (col(column).isNull()) |
                    (size(col(column)) == 0)
                ).count()
            else:
                # For other types, check all conditions
                missing_count = df.filter(
                    (col(column).isNull()) |
                    (col(column) == "") |
                    (trim(col(column)) == "") |
                    (lower(col(column)) == "nan") |
                    (col(column) == "null") |
                    (col(column) == "NULL") |
                    (col(column) == "None") |
                    (col(column) == "N/A") |
                    (col(column) == "n/a") |
                    (col(column) == "[]") |
                    (col(column) == "{}") |
                    (col(column) == "undefined") |
                    (col(column) == "UNDEFINED")
                ).count()
            
            null_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
            
            if null_percentage > 30:
                flag = "Red"
            elif null_percentage > 10:
                flag = "Yellow"
            else:
                flag = "Green"
            
            quality_data_report["datasource"].append(file_name)
            quality_data_report["column"].append(column)
            quality_data_report["null_count"].append(missing_count)
            quality_data_report["count"].append(total_rows)
            quality_data_report["null_percentage"].append(round(null_percentage, 2))
            quality_data_report["flag"].append(flag)
    
    if not quality_data_report["datasource"]:
        return None
    
    quality_schema = StructType([
        StructField("datasource", StringType(), True),
        StructField("column", StringType(), True),
        StructField("null_count", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("null_percentage", DoubleType(), True),
        StructField("flag", StringType(), True)
    ])
    
    df_quality_report = spark.createDataFrame(
        list(zip(
            quality_data_report["datasource"],
            quality_data_report["column"],
            quality_data_report["null_count"],
            quality_data_report["count"],
            quality_data_report["null_percentage"],
            quality_data_report["flag"]
        )),
        quality_schema
    )
    
    df_quality_report.write.partitionBy("flag").parquet("hdfs://master:8080/opt/spark/data/bronze_layer/data_quality/data_quality_report", mode="overwrite")
    
    return df_quality_report

df_quality_report = null_data_report()
spark.stop()
        
