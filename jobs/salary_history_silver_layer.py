from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, LongType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, concat_ws

spark = SparkSession.builder.appName('Salary-Tech-Company-Application').getOrCreate()

# Schema definition
salary_history_schema = StructType([
    StructField("id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("salary", LongType(), True),
    StructField("effective_date", DateType(), True),
    StructField("change_reason", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("position", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("bonus_amount", LongType(), True),
    StructField("stock_options", LongType(), True)
])

# Ler o arquivo parquet
df_salary_history = spark.read.schema(salary_history_schema).parquet("hdfs:///opt/spark/data/bronze_layer/salary_history.parquet")


# Add timestamp column
df_salary_history = df_salary_history.withColumn('received_at', current_timestamp())

# 1. Ensure referential integrity (not null/not empty for key fields)
df_salary_history = df_salary_history.filter(
    (col("id").isNotNull()) & (trim(col("id")) != "")
)
df_salary_history = df_salary_history.filter(
    (col("employee_id").isNotNull()) & (trim(col("employee_id")) != "")
)
df_salary_history = df_salary_history.filter(
    (col("department_id").isNotNull()) & (trim(col("department_id")) != "")
)

# 2. Handle null/empty dates
df_salary_history = df_salary_history.withColumn(
    "effective_date",
    when(col("effective_date").isNull() | (trim(col("effective_date").cast("string")) == ""), "0000-01-01").otherwise(col("effective_date"))
)

# 3. Handle null/empty numbers
df_salary_history = df_salary_history.withColumn(
    "salary",
    when(col("salary").isNull(), 0).otherwise(col("salary"))
)
df_salary_history = df_salary_history.withColumn(
    "bonus_amount",
    when(col("bonus_amount").isNull(), 0).otherwise(col("bonus_amount"))
)
df_salary_history = df_salary_history.withColumn(
    "stock_options",
    when(col("stock_options").isNull(), 0).otherwise(col("stock_options"))
)

# 4. Handle null/empty strings
df_salary_history = df_salary_history.withColumn(
    "change_reason",
    when(col("change_reason").isNull() | (trim(col("change_reason")) == ""), "No reason provided").otherwise(col("change_reason"))
)

df_salary_history = df_salary_history.withColumn(
    "position",
    when(col("position").isNull() | (trim(col("position")) == ""), "Unknown").otherwise(col("position"))
)

df_salary_history = df_salary_history.withColumn(
    "currency",
    when(col("currency").isNull() | (trim(col("currency")) == ""), "Unknown").otherwise(col("currency"))
)

df_salary_history.write.parquet("hdfs:///opt/spark/data/silver_layer/salary_history.parquet", mode="overwrite")
