from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp

spark = SparkSession.builder.appName('Tech-Company-Application').getOrCreate()

employee_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("position", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("performance_score", FloatType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("education", StringType(), True),
    StructField("years_experience", IntegerType(), True),
    StructField("certifications", ArrayType(StringType()), True),
    StructField("projects_assigned", IntegerType(), True),
    StructField("is_manager", BooleanType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("emergency_contact", StringType(), True),
    StructField("emergency_phone", StringType(), True),
    StructField("work_location", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("last_review_date", StringType(), True),
    StructField("next_review_date", StringType(), True),
    StructField("attendance_rate", FloatType(), True),
    StructField("overtime_hours", IntegerType(), True),
    StructField("training_hours", IntegerType(), True),
    StructField("sick_days", IntegerType(), True),
    StructField("vacation_days_used", IntegerType(), True),
    StructField("vacation_days_remaining", IntegerType(), True),
])

df_employee = spark.read.schema(employee_schema).json('hdfs:///opt/spark/data/bronze_layer/employees.json', multiLine=True)

cols_to_drop = ['emergency_contact', 'email', 'name', "phone", "address", "emergency_phone", 'sick_days', 'attendance_rate', 'training_hours']
df_employee = df_employee.drop(*cols_to_drop)

df_employee = df_employee.withColumn('received_at', current_timestamp())

# REFERENCIAL INTEGRITY DOES NOT ALLOW NULL OR EMPTY VALUES
df_employee = df_employee.filter(
    (col("id").isNotNull()) & (trim(col("id")) != "")
)

df_employee = df_employee.filter(
    (col("department_id").isNotNull()) & (trim(col("department_id")) != "")
)

df_employee = df_employee.filter(
    (col("employment_type").isNotNull()) & (trim(col("employment_type")) != "")
)

df_employee = df_employee.filter(
    (col("education").isNotNull()) & (trim(col("education")) != "")
)
df_employee = df_employee.filter(
    (col("hire_date").isNotNull()) & (trim(col("hire_date")) != "")
)

df_employee = df_employee.withColumn(
    "hire_date",
    when(col("hire_date").isNull() | (trim(col("hire_date")) == ""), "0000-01-01").otherwise(col("hire_date"))
)
df_employee = df_employee.withColumn(
    "last_review_date",
    when(col("last_review_date").isNull() | (trim(col("last_review_date")) == ""), "0000-01-01").otherwise(col("last_review_date"))
)

df_employee = df_employee.withColumn(
    "next_review_date",
    when(col("next_review_date").isNull() | (trim(col("next_review_date")) == ""), "0000-01-01").otherwise(col("next_review_date"))
)

df_employee = df_employee.withColumn(
    "skills",
    when(
        col("skills").isNull() | (size(col("skills")) == 0),
        array(lit("No skills"))
    ).otherwise(col("skills"))
)

df_employee = df_employee.withColumn(
    "skills",
    when(
        col("skills").isNull() | (size(col("skills")) == 0),
        array(lit("No skills"))
    ).otherwise(col("skills"))
)

df_employee = df_employee.withColumn(
    "projects_assigned",
    when(
        col("projects_assigned").isNull() | (trim(col("projects_assigned")) == 0),
        0
    ).otherwise(col("projects_assigned"))
)

df_employee = df_employee.withColumn(
    "projects_assigned",
    when(
        col("projects_assigned").isNull() | (trim(col("projects_assigned")) == 0),
        0
    ).otherwise(col("projects_assigned"))
)

df_employee = df_employee.withColumn(
    "overtime_hours",
    when(
        col("overtime_hours").isNull() | (trim(col("overtime_hours")) == 0),
        0
    ).otherwise(col("overtime_hours"))
)

df_employee = df_employee.withColumn(
    "vacation_days_used",
    when(
        col("vacation_days_used").isNull() | (trim(col("vacation_days_used")) == 0),
        0
    ).otherwise(col("vacation_days_used"))
)

df_employee = df_employee.withColumn(
    "vacation_days_remaining",
    when(
        col("vacation_days_remaining").isNull() | (trim(col("vacation_days_remaining")) == 0),
        0
    ).otherwise(col("vacation_days_remaining"))
)

df_employee = df_employee.withColumn(
    "years_experience",
    when(
        col("years_experience").isNull() | (trim(col("years_experience")) == 0),
        0
    ).otherwise(col("years_experience"))
)

df_employee = df_employee.withColumn(
    "is_manager",
    when(
        col("is_manager").isNull() | (trim(col("is_manager")) == 0),
        False
    ).otherwise(col("is_manager"))
)

df_employee = df_employee.withColumn(
    "position",
    when(
        col("position").isNull() | (trim(col("position")) == ""),
        "Unknown"
    ).otherwise(col("position"))
)

df_employee = df_employee.withColumn(
    "work_location",
    when(
        col("work_location").isNull() | (trim(col("work_location")) == ""),
        "Unknown"
    ).otherwise(col("work_location"))
)

df_employee.write.parquet("hdfs:///opt/spark/data/silver_layer/employee.parquet", mode="overwrite")