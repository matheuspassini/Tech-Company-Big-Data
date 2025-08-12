#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, avg, count, sum, round, countDistinct, collect_list, collect_set, size, split, min as spark_min, max as spark_max, when, expr, array_sort, array_contains

spark = SparkSession.builder.appName('Department-Analytics-Gold-Application-Cluster').getOrCreate()

df_employees = spark.read.parquet("hdfs://master:8080/opt/spark/data/silver_layer/employee.parquet")
df_departments = spark.read.parquet("hdfs://master:8080/opt/spark/data/silver_layer/departments.parquet")

# Join employees and departments
df_joined = df_employees.join(
    df_departments,
    df_employees.department_id == df_departments.id,
    "inner"
)

# Add education level hierarchy for proper minimum calculation
df_joined = df_joined.withColumn(
    "education_level_numeric",
    when(col("education") == "High School", 1)
    .when(col("education") == "Associate's Degree", 2)
    .when(col("education") == "Bachelor's Degree", 3)
    .when(col("education") == "Master's Degree", 4)
    .when(col("education") == "PhD", 5)
    .otherwise(0)
)

# Create analytics table with aggregations
df_analytics = df_joined.groupBy(
    df_departments.region.alias("region"),
    df_departments.id.alias("department_id"),
    df_departments.name.alias("department_name"),
    col("year_hire_date").alias("hire_year")
).agg(
    # Position analysis
    countDistinct("position").alias("total_positions"),
    collect_set("position").alias("all_positions"),
    
    # Employee counts
    count("*").alias("total_employees"),
    
    # Skills analysis
    avg(size(split(col("skills"), ","))).alias("avg_skills_per_employee"),
    
    # Budget and financial metrics
    sum("budget").alias("total_budget"),
    avg("budget").alias("avg_budget"),
    
    # Certification analysis
    avg(size(split(col("certifications"), ","))).alias("avg_certifications_per_employee"),
    sum(size(split(col("certifications"), ","))).alias("total_certifications"),
    
    # Experience analysis
    avg("years_experience").alias("avg_years_experience"),
    spark_min("years_experience").alias("min_years_experience"),
    spark_max("years_experience").alias("max_years_experience"),
    
    # Education analysis
    spark_max("education_level_numeric").alias("maximum_education_level_numeric"),
    
    # Salary analysis
    sum("salary").alias("total_salary_cost"),
    avg("salary").alias("avg_salary"),
    spark_min("salary").alias("min_salary"),
    spark_max("salary").alias("max_salary"),
    
    # Performance metrics
    avg("performance_score").alias("avg_performance"),
    avg("projects_assigned").alias("avg_projects_per_employee"),
    sum("projects_assigned").alias("total_projects"),
    
    # Work metrics
    avg("overtime_hours").alias("avg_overtime_hours"),
    sum("overtime_hours").alias("total_overtime_hours"),
    
    # Vacation metrics
    avg("vacation_days_used").alias("avg_vacation_used"),
    avg("vacation_days_remaining").alias("avg_vacation_remaining")
).withColumn(
    "avg_skills_per_employee", round(col("avg_skills_per_employee"))
).withColumn(
    "avg_certifications_per_employee", round(col("avg_certifications_per_employee"))
).withColumn(
    "avg_years_experience", round(col("avg_years_experience"))
).withColumn(
    "avg_salary", round(col("avg_salary"))
).withColumn(
    "avg_performance", round(col("avg_performance"))
).withColumn(
    "avg_projects_per_employee", round(col("avg_projects_per_employee"))
).withColumn(
    "avg_overtime_hours", round(col("avg_overtime_hours"))
).withColumn(
    "avg_vacation_used", round(col("avg_vacation_used"))
).withColumn(
    "avg_vacation_remaining", round(col("avg_vacation_remaining"))
).withColumn(
    "salary_budget_ratio", round(col("total_salary_cost") / col("total_budget") * 100, 2)
).withColumn(
    "maximum_education_level",
    when(col("maximum_education_level_numeric") == 1, "High School")
    .when(col("maximum_education_level_numeric") == 2, "Associate's Degree")
    .when(col("maximum_education_level_numeric") == 3, "Bachelor's Degree")
    .when(col("maximum_education_level_numeric") == 4, "Master's Degree")
    .when(col("maximum_education_level_numeric") == 5, "PhD")
    .otherwise("Unknown")
).withColumn(
    "department_health",
    when(col("salary_budget_ratio") > 1, "Red Flag")
    .when(col("salary_budget_ratio") < 1, "Green Flag")
    .when(col("salary_budget_ratio") == 1, "Yellow Flag")
    .otherwise("Gray Flag - To Review")
).withColumn(
    "data_quality_issues",
    when(
        (array_contains(col("all_positions"), "Unknown")) |
        (col("maximum_education_level") == "Unknown"),
        "Position/Education data needs review"
    ).when(
        array_contains(col("all_positions"), "Unknown"),
        "Position data needs review"
    ).when(
        col("maximum_education_level") == "Unknown",
        "Education data needs review"
    ).otherwise("No data quality issues detected")
).drop("education_level_numeric", "maximum_education_level_numeric").withColumn(
    "gold_processed_at", current_timestamp()
)

# Write to Gold layer in Parquet format with partitioning
df_analytics.write.partitionBy("region", "department_name", "hire_year").parquet("hdfs://master:8080/opt/spark/data/gold_layer/department_analytics.parquet", mode="overwrite")

spark.stop() 