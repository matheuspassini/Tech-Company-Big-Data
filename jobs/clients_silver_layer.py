from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import col, when, current_timestamp

spark = SparkSession.builder.appName('Clients-Tech-Company-Application').getOrCreate()

# 0. Schema definition
clients_schema = StructType([
    StructField("id", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("contact_person", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("country", StringType(), True),
    StructField("contract_start_date", StringType(), True),
    StructField("contract_value", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("account_manager_id", StringType(), True),
    StructField("annual_revenue", IntegerType(), True),
    StructField("employee_count", IntegerType(), True),
    StructField("last_contact_date", StringType(), True),
    StructField("customer_since", StringType(), True),
    StructField("churn_risk_score", FloatType(), True),
    StructField("satisfaction_score", FloatType(), True),
    StructField("payment_history", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("upsell_potential", FloatType(), True)
])

# 1. Read CSV file with header
df_clients = spark.read.schema(clients_schema).csv('hdfs:///opt/spark/data/bronze_layer/clients.csv', header=True)

# 2. Transform empty strings in null values
df_clients = df_clients.select([
    when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_clients.columns
])

# 3. Add timestamp column
df_clients = df_clients.withColumn('received_at', current_timestamp())

# 4. Drop null values (Referential Integrity)
df_clients = df_clients.filter(col("id").isNotNull())
df_clients = df_clients.filter(col("account_manager_id").isNotNull())

# 5. Date transformations
df_clients = df_clients.withColumn(
    "contract_start_date",
    when(col("contract_start_date").isNull(), "0000-01-01").otherwise(col("contract_start_date"))
)
df_clients = df_clients.withColumn(
    "last_contact_date",
    when(col("last_contact_date").isNull(), "0000-01-01").otherwise(col("last_contact_date"))
)
df_clients = df_clients.withColumn(
    "customer_since",
    when(col("customer_since").isNull(), "0000-01-01").otherwise(col("customer_since"))
)

# 6. Number transformations
df_clients = df_clients.withColumn(
    "contract_value",
    when(col("contract_value").isNull(), 0).otherwise(col("contract_value"))
)
df_clients = df_clients.withColumn(
    "annual_revenue",
    when(col("annual_revenue").isNull(), 0).otherwise(col("annual_revenue"))
)
df_clients = df_clients.withColumn(
    "employee_count",
    when(col("employee_count").isNull(), 0).otherwise(col("employee_count"))
)

# 7. String transformations
df_clients = df_clients.withColumn(
    "churn_risk_score",
    when(col("churn_risk_score").isNull(), 0.0).otherwise(col("churn_risk_score"))
)
df_clients = df_clients.withColumn(
    "satisfaction_score",
    when(col("satisfaction_score").isNull(), 0.0).otherwise(col("satisfaction_score"))
)
df_clients = df_clients.withColumn(
    "upsell_potential",
    when(col("upsell_potential").isNull(), 0.0).otherwise(col("upsell_potential"))
)

# 8. String transformations
df_clients = df_clients.withColumn(
    "company_name",
    when(col("company_name").isNull(), "Unknown").otherwise(col("company_name"))
)
df_clients = df_clients.withColumn(
    "industry",
    when(col("industry").isNull(), "Unknown").otherwise(col("industry"))
)
df_clients = df_clients.withColumn(
    "contact_person",
    when(col("contact_person").isNull(), "Unknown").otherwise(col("contact_person"))
)
df_clients = df_clients.withColumn(
    "email",
    when(col("email").isNull(), "Unknown").otherwise(col("email"))
)
df_clients = df_clients.withColumn(
    "phone",
    when(col("phone").isNull(), "Unknown").otherwise(col("phone"))
)
df_clients = df_clients.withColumn(
    "address",
    when(col("address").isNull(), "Unknown").otherwise(col("address"))
)
df_clients = df_clients.withColumn(
    "country",
    when(col("country").isNull(), "Unknown").otherwise(col("country"))
)
df_clients = df_clients.withColumn(
    "status",
    when(col("status").isNull(), "Unknown").otherwise(col("status"))
)
df_clients = df_clients.withColumn(
    "payment_history",
    when(col("payment_history").isNull(), "Unknown").otherwise(col("payment_history"))
)
df_clients = df_clients.withColumn(
    "contract_type",
    when(col("contract_type").isNull(), "Unknown").otherwise(col("contract_type"))
)

# 9. Write to parquet
df_clients.write.parquet("hdfs:///opt/spark/data/silver_layer/clients.parquet", mode="overwrite")