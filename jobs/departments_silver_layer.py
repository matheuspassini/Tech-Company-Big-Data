from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp

spark = SparkSession.builder.appName('Tech-Company-Application').getOrCreate()

# Schema definition
departments_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("budget", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("manager_id", StringType(), True),
    StructField("headcount", IntegerType(), True),
    StructField("founded_date", StringType(), True),
    StructField("description", StringType(), True),
    StructField("tech_stack", StringType(), True),
    StructField("quarterly_budget", IntegerType(), True),
    StructField("yearly_revenue", IntegerType(), True),
    StructField("cost_center", StringType(), True),
    StructField("region", StringType(), True),
    StructField("office_size", IntegerType(), True),
    StructField("last_audit_date", StringType(), True),
])

# 0. Read CSV file with header
df_departments = spark.read.schema(departments_schema).csv('hdfs:///opt/spark/data/bronze_layer/departments.csv', header=True)

# 1. Transform empty strings in null values
df_departments = df_departments.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_departments.columns
])

# 2. Add timestamp column
df_departments = df_departments.withColumn('received_at', current_timestamp())

# 3. Drop null values (Referential Integrity)
df_departments = df_departments.filter(col("id").isNotNull())
df_departments = df_departments.filter(col("manager_id").isNotNull())

# 4. Date transformations
df_departments = df_departments.withColumn(
    "founded_date",
    when(col("founded_date").isNull(), "0000-01-01").otherwise(col("founded_date"))
)
df_departments = df_departments.withColumn(
    "last_audit_date",
    when(col("last_audit_date").isNull(), "0000-01-01").otherwise(col("last_audit_date"))
)

# 5. Number transformations
df_departments = df_departments.withColumn(
    "budget",
    when(col("budget").isNull(), 0).otherwise(col("budget")))
df_departments = df_departments.withColumn(
    "headcount",
    when(col("headcount").isNull(), 0).otherwise(col("headcount"))
)
df_departments = df_departments.withColumn(
    "quarterly_budget",
    when(col("quarterly_budget").isNull(), 0).otherwise(col("quarterly_budget"))
)
df_departments = df_departments.withColumn(
    "yearly_revenue",
    when(col("yearly_revenue").isNull(), 0).otherwise(col("yearly_revenue"))
)
df_departments = df_departments.withColumn(
    "office_size",
    when(col("office_size").isNull(), 0).otherwise(col("office_size"))
)

# 6. Array transformations
df_departments = df_departments.withColumn(
    "tech_stack",
    when(
        col("tech_stack").isNull(),
        array(lit("No tech stack"))
    ).otherwise(
        split(
            regexp_replace(
                regexp_replace(col("tech_stack"), "\\[|\\]", ""),
                "'", ""
            ),
            ", "
        )
    )
)

# 7. String transformations
df_departments = df_departments.withColumn(
    "location",
    when(col("location").isNull(), "Unknown").otherwise(col("location"))
)
df_departments = df_departments.withColumn(
    "region",
    when(col("region").isNull(), "Unknown").otherwise(col("region"))
)
df_departments = df_departments.withColumn(
    "description",
    when(col("description").isNull(), "No description available").otherwise(col("description"))
)

# Write to parquet
df_departments.write.parquet("hdfs:///opt/spark/data/silver_layer/departments.parquet", mode="overwrite")