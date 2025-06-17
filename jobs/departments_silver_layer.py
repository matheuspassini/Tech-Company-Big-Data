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

# Read CSV file with header
df_departments = spark.read.schema(departments_schema).csv('hdfs:///opt/spark/data/bronze_layer/departments.csv', header=True)

# 1. Drop columns (if needed)
# cols_to_drop = ['column1', 'column2']
# df_departments = df_departments.drop(*cols_to_drop)

# Add timestamp column
df_departments = df_departments.withColumn('received_at', current_timestamp())

# 2. Drop null/empty values (Referential Integrity)
df_departments = df_departments.filter(
    (col("id").isNotNull()) & (trim(col("id")) != "")
)

df_departments = df_departments.filter(
    (col("manager_id").isNotNull()) & (trim(col("manager_id")) != "")
)


# 3. Date transformations
df_departments = df_departments.withColumn(
    "founded_date",
    when(col("founded_date").isNull() | (trim(col("founded_date")) == ""), "0000-01-01").otherwise(col("founded_date"))
)

df_departments = df_departments.withColumn(
    "last_audit_date",
    when(col("last_audit_date").isNull() | (trim(col("last_audit_date")) == ""), "0000-01-01").otherwise(col("last_audit_date"))
)

# 4. Number transformations
df_departments = df_departments.withColumn(
    "budget",
    when(
        col("budget").isNull() | (trim(col("budget")) == 0),
        0
    ).otherwise(col("budget"))
)

df_departments = df_departments.withColumn(
    "headcount",
    when(
        col("headcount").isNull() | (trim(col("headcount")) == 0),
        0
    ).otherwise(col("headcount"))
)

df_departments = df_departments.withColumn(
    "quarterly_budget",
    when(
        col("quarterly_budget").isNull() | (trim(col("quarterly_budget")) == 0),
        0
    ).otherwise(col("quarterly_budget"))
)

df_departments = df_departments.withColumn(
    "yearly_revenue",
    when(
        col("yearly_revenue").isNull() | (trim(col("yearly_revenue")) == 0),
        0
    ).otherwise(col("yearly_revenue"))
)

df_departments = df_departments.withColumn(
    "office_size",
    when(
        col("office_size").isNull() | (trim(col("office_size")) == 0),
        0
    ).otherwise(col("office_size"))
)

# 5. Array transformations
df_departments = df_departments.withColumn(
    "tech_stack",
    when(
        col("tech_stack").isNull() | (trim(col("tech_stack")) == ""),
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

# 6. String transformations
df_departments = df_departments.withColumn(
    "location",
    when(
        col("location").isNull() | (trim(col("location")) == ""),
        "Unknown"
    ).otherwise(col("location"))
)

df_departments = df_departments.withColumn(
    "region",
    when(
        col("region").isNull() | (trim(col("region")) == ""),
        "Unknown"
    ).otherwise(col("region"))
)

df_departments = df_departments.withColumn(
    "description",
    when(
        col("description").isNull() | (trim(col("description")) == ""),
        "No description available"
    ).otherwise(col("description"))
)

# Write to parquet
df_departments.write.parquet("hdfs:///opt/spark/data/silver_layer/departments.parquet", mode="overwrite")