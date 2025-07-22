from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import col, when, current_timestamp
from transformations.clients_transformations import clean_null_dates, clean_null_numbers, clean_null_floats, clean_null_strings

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

# 5. Apply modularized transformations
df_clients = clean_null_dates(df_clients)
df_clients = clean_null_numbers(df_clients)
df_clients = clean_null_floats(df_clients)
df_clients = clean_null_strings(df_clients)

# 6. Write to CSV
df_clients.write.csv("hdfs:///opt/spark/data/silver_layer/clients.csv", mode="overwrite", header=True)