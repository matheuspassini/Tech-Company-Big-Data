from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, trim, when, regexp_replace, split, expr, size, array, lit, current_timestamp, concat_ws

spark = SparkSession.builder.appName('Tasks-Tech-Company-Application').getOrCreate()

# Schema definition
tasks_schema = StructType([
    StructField("id", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("name", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("assigned_to", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("due_date", StringType(), True),
    StructField("estimated_hours", IntegerType(), True),
    StructField("actual_hours", IntegerType(), True),
    StructField("dependencies", StringType(), True),
    StructField("tags", StringType(), True)
])

# Read JSON file with header
df_tasks = spark.read.schema(tasks_schema).json('hdfs:///opt/spark/data/bronze_layer/tasks.json', multiLine=True)

# Add timestamp column
df_tasks = df_tasks.withColumn('received_at', current_timestamp())

# 2. Drop null/empty values (Referential Integrity)
df_tasks = df_tasks.filter(
    (col("id").isNotNull()) & (trim(col("id")) != "")
)

df_tasks = df_tasks.filter(
    (col("project_id").isNotNull()) & (trim(col("project_id")) != "")
)

df_tasks = df_tasks.filter(
    (col("assigned_to").isNotNull()) & (trim(col("assigned_to")) != "")
)

# 3. Date transformations
df_tasks = df_tasks.withColumn(
    "created_date",
    when(col("created_date").isNull() | (trim(col("created_date")) == ""), "0000-01-01").otherwise(col("created_date"))
)

df_tasks = df_tasks.withColumn(
    "due_date",
    when(col("due_date").isNull() | (trim(col("due_date")) == ""), "0000-01-01").otherwise(col("due_date"))
)

# 4. Number transformations
df_tasks = df_tasks.withColumn(
    "estimated_hours",
    when(
        col("estimated_hours").isNull() | (trim(col("estimated_hours")) == 0),
        0
    ).otherwise(col("estimated_hours"))
)

df_tasks = df_tasks.withColumn(
    "actual_hours",
    when(
        col("actual_hours").isNull() | (trim(col("actual_hours")) == 0),
        0
    ).otherwise(col("actual_hours"))
)

df_tasks = df_tasks.withColumn(
    "dependencies",
    when(
        col("dependencies").isNull() | (trim(col("dependencies")) == 0),
        0
    ).otherwise(col("dependencies"))
)

# 5. Array transformations
df_tasks = df_tasks.withColumn(
    "tags",
    when(
        col("tags").isNull() | (trim(col("tags")) == ""),
        array(lit("No tags"))
    ).otherwise(
        split(col("tags"), ",\\s*")
    )
)

df_tasks = df_tasks.withColumn(
    "tags",
    when(
        size(col("tags")) == 0,
        array(lit("No tags"))
    ).otherwise(col("tags"))
)

# 6. String transformations
df_tasks = df_tasks.withColumn(
    "name",
    when(
        col("name").isNull() | (trim(col("name")) == ""),
        "Unknown"
    ).otherwise(col("name"))
)

df_tasks = df_tasks.withColumn(
    "description",
    when(
        col("description").isNull() | (trim(col("description")) == ""),
        "No description available"
    ).otherwise(col("description"))
)

df_tasks = df_tasks.withColumn(
    "status",
    when(
        col("status").isNull() | (trim(col("status")) == ""),
        "No status available"
    ).otherwise(col("status"))
)

df_tasks = df_tasks.withColumn(
    "priority",
    when(
        col("priority").isNull() | (trim(col("priority")) == ""),
        "Low"
    ).otherwise(col("priority"))
)

# Write to parquet
df_tasks.write.parquet("hdfs:///opt/spark/data/silver_layer/tasks.parquet", mode="overwrite")
