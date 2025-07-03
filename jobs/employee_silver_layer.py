from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp

spark = SparkSession.builder.appName('Tech-Company-Application').getOrCreate()

# Schema definition
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

# 0. Read JSON file with header
df_employee = spark.read.schema(employee_schema).json('hdfs:///opt/spark/data/bronze_layer/employees.json', multiLine=True)

# 1. Transform empty strings in null values
df_employee = df_employee.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_employee.columns
])

# 2. Drop columns
cols_to_drop = ['emergency_contact', 'email', 'name', "phone", "address", "emergency_phone", 'sick_days', 'attendance_rate', 'training_hours']
df_employee = df_employee.drop(*cols_to_drop)

# 3. Add timestamp column
df_employee = df_employee.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_employee = df_employee.filter(col("id").isNotNull())
df_employee = df_employee.filter(col("department_id").isNotNull())
df_employee = df_employee.filter(col("employment_type").isNotNull())
df_employee = df_employee.filter(col("education").isNotNull())
df_employee = df_employee.filter(col("hire_date").isNotNull())

# 5. Date transformations
df_employee = df_employee.withColumn(
    "hire_date",
    when(col("hire_date").isNull(), "0000-01-01").otherwise(col("hire_date"))
)
df_employee = df_employee.withColumn(
    "last_review_date",
    when(col("last_review_date").isNull(), "0000-01-01").otherwise(col("last_review_date"))
)
df_employee = df_employee.withColumn(
    "next_review_date",
    when(col("next_review_date").isNull(), "0000-01-01").otherwise(col("next_review_date"))
)

# 6. Number transformations
df_employee = df_employee.withColumn(
    "projects_assigned",
    when(col("projects_assigned").isNull(), 0).otherwise(col("projects_assigned"))
)
df_employee = df_employee.withColumn(
    "overtime_hours",
    when(col("overtime_hours").isNull(), 0).otherwise(col("overtime_hours"))
)
df_employee = df_employee.withColumn(
    "vacation_days_used",
    when(col("vacation_days_used").isNull(), 0).otherwise(col("vacation_days_used"))
)
df_employee = df_employee.withColumn(
    "vacation_days_remaining",
    when(col("vacation_days_remaining").isNull(), 0).otherwise(col("vacation_days_remaining"))
)
df_employee = df_employee.withColumn(
    "years_experience",
    when(col("years_experience").isNull(), 0).otherwise(col("years_experience"))
)

# 7. Array transformations
df_employee = df_employee.withColumn(
    "skills",
    when(
        col("skills").isNull() | (size(col("skills")) == 0),
        array(lit("No skills"))
    ).otherwise(col("skills"))
)
df_employee = df_employee.withColumn(
    "certifications",
    when(
        col("certifications").isNull() | (size(col("certifications")) == 0),
        array(lit("No certifications"))
    ).otherwise(col("certifications"))
)

# 8. String transformations
df_employee = df_employee.withColumn(
    "is_manager",
    when(col("is_manager").isNull(), False).otherwise(col("is_manager"))
)
df_employee = df_employee.withColumn(
    "position",
    when(col("position").isNull(), "Unknown").otherwise(col("position"))
)
df_employee = df_employee.withColumn(
    "work_location",
    when(col("work_location").isNull(), "Unknown").otherwise(col("work_location"))
)

# 9. Write to parquet
df_employee.write.parquet("hdfs:///opt/spark/data/silver_layer/employee.parquet", mode="overwrite")