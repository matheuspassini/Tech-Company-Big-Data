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

# 0. Read Parquet file
df_salary_history = spark.read.schema(salary_history_schema).parquet("hdfs:///opt/spark/data/bronze_layer/salary_history.parquet")

# 1. Transform empty strings in null values
df_salary_history = df_salary_history.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_salary_history.columns
])

# 2. Add timestamp column
df_salary_history = df_salary_history.withColumn('received_at', current_timestamp())

# 3. Drop null values (Referential Integrity)
df_salary_history = df_salary_history.filter(col("id").isNotNull())
df_salary_history = df_salary_history.filter(col("employee_id").isNotNull())
df_salary_history = df_salary_history.filter(col("department_id").isNotNull())

# 4. Handle null dates
df_salary_history = df_salary_history.withColumn(
    "effective_date",
    when(col("effective_date").isNull(), "0000-01-01").otherwise(col("effective_date"))
)

# 5. Handle null numbers
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

# 6. Handle null strings
df_salary_history = df_salary_history.withColumn(
    "change_reason",
    when(col("change_reason").isNull(), "No reason provided").otherwise(col("change_reason"))
)
df_salary_history = df_salary_history.withColumn(
    "position",
    when(col("position").isNull(), "Unknown").otherwise(col("position"))
)
df_salary_history = df_salary_history.withColumn(
    "currency",
    when(col("currency").isNull(), "Unknown").otherwise(col("currency"))
)

df_salary_history.write.parquet("hdfs:///opt/spark/data/silver_layer/salary_history.parquet", mode="overwrite")
