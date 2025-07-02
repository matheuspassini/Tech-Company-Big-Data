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

# 0. Read JSON file with header
df_tasks = spark.read.schema(tasks_schema).json('hdfs:///opt/spark/data/bronze_layer/tasks.json', multiLine=True)

# 1. Transform empty strings in null values
df_tasks = df_tasks.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_tasks.columns
])

# 2. Add timestamp column
df_tasks = df_tasks.withColumn('received_at', current_timestamp())

# 3. Drop null values (Referential Integrity)
df_tasks = df_tasks.filter(
    col("id").isNotNull()
)
df_tasks = df_tasks.filter(
    col("project_id").isNotNull()
)
df_tasks = df_tasks.filter(
    col("assigned_to").isNotNull()
)

# 4. Date transformations
df_tasks = df_tasks.withColumn(
    "created_date",
    when(col("created_date").isNull(), "0000-01-01").otherwise(col("created_date"))
)

df_tasks = df_tasks.withColumn(
    "due_date",
    when(col("due_date").isNull(), "0000-01-01").otherwise(col("due_date"))
)

# 5. Number transformations
df_tasks = df_tasks.withColumn(
    "estimated_hours",
    when(
        col("estimated_hours").isNull(),
        0
    ).otherwise(col("estimated_hours"))
)

df_tasks = df_tasks.withColumn(
    "actual_hours",
    when(
        col("actual_hours").isNull(),
        0
    ).otherwise(col("actual_hours"))
)

df_tasks = df_tasks.withColumn(
    "dependencies",
    when(
        col("dependencies").isNull(),
        0
    ).otherwise(col("dependencies"))
)

# 6. Array transformations
df_tasks = df_tasks.withColumn(
    "tags",
    when(
        col("tags").isNull(),
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

# 7. String transformations
df_tasks = df_tasks.withColumn(
    "name",
    when(
        col("name").isNull(),
        "Unknown"
    ).otherwise(col("name"))
)

df_tasks = df_tasks.withColumn(
    "description",
    when(
        col("description").isNull(),
        "No description available"
    ).otherwise(col("description"))
)

df_tasks = df_tasks.withColumn(
    "status",
    when(
        col("status").isNull(),
        "No status available"
    ).otherwise(col("status"))
)

df_tasks = df_tasks.withColumn(
    "priority",
    when(
        col("priority").isNull(),
        "Low"
    ).otherwise(col("priority"))
)

# Write to parquet
df_tasks.write.parquet("hdfs:///opt/spark/data/silver_layer/tasks.parquet", mode="overwrite")
